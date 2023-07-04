const express = require("express");
const sqlite3 = require("sqlite3").verbose();
const http = require("http");

const app = express();
app.use(express.json());

const port = process.env.PORT || 3000;
const server = http.createServer(app);

// Database configuration
const db = new sqlite3.Database(":memory:");

// Create Contacts table
db.serialize(() => {
	db.run(`CREATE TABLE IF NOT EXISTS Contact (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    phoneNumber TEXT,
    email TEXT,
    linkedId INTEGER,
    linkPrecedence TEXT NOT NULL,
    createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deletedAt TIMESTAMP
  )`);
});

// Define the API endpoint
app.post("/identify", async (req, res) => {
	console.log("Request:", req.body);
	const { email, phoneNumber } = req.body;

	try {
		db.serialize(() => {
			db.all(
				`SELECT * FROM Contact WHERE email = ? OR phoneNumber = ?`,
				[email, phoneNumber],
				async (err, existingContacts) => {
					if (err) {
						console.error("Error:", err);
						return res
							.status(500)
							.json({ error: "Internal Server Error" });
					}

					if (existingContacts.length > 0) {
						try {
							const updatedContacts = await new Promise(
								(resolve, reject) => {
									db.all(
										`SELECT * FROM Contact WHERE id = ? OR linkedId = ? OR email = ? OR phoneNumber = ?`,
										[
											existingContacts[0].linkedId,
											existingContacts[0].id,
											email,
											phoneNumber,
										],
										(err, _existingContacts) => {
											if (err) {
												console.error("Error:", err);
												reject(err);
											}
											resolve(_existingContacts);
										}
									);
								}
							);
							existingContacts = updatedContacts;
						} catch (err) {
							console.error("Error:", err);
							return res
								.status(500)
								.json({ error: "Internal Server Error" });
						}

						const primaryContacts = existingContacts.filter(
							(contact) => contact.linkPrecedence === "primary"
						);
						const secondaryContacts = existingContacts
							.filter(
								(contact) =>
									contact.linkPrecedence === "secondary"
							)
							.map((contact) => contact.id);

						const emails = [
							...new Set(
								existingContacts.map((contact) => contact.email)
							),
						];
						const phoneNumbers = [
							...new Set(
								existingContacts.map(
									(contact) => contact.phoneNumber
								)
							),
						];

						if (primaryContacts.length > 1) {
							// Convert the earlier contact to primary and update the later contact as secondary
							const earlierPrimaryContact = primaryContacts[0];
							const laterPrimaryContact = primaryContacts[1];

							// Update the later contact as secondary and link it to the earlier contact
							db.run(
								`UPDATE Contact SET linkPrecedence = 'secondary', linkedId = ? WHERE id = ?`,
								[
									earlierPrimaryContact.id,
									laterPrimaryContact.id,
								],
								(err) => {
									if (err) {
										console.error("Error:", err);
										return res.status(500).json({
											error: "Internal Server Error",
										});
									}

									const secondaryContactId =
										laterPrimaryContact.id;

									// Consolidate the contacts
									const consolidatedContact = {
										primaryContactId:
											earlierPrimaryContact.id,
										emails: emails.filter(Boolean),
										phoneNumbers:
											phoneNumbers.filter(Boolean),
										secondaryContactIds: [
											...secondaryContacts,
											secondaryContactId,
										],
									};

									console.log(
										"Update Linked Preference Response:",
										JSON.stringify(consolidatedContact)
									);
									res.json({ contact: consolidatedContact });
								}
							);
						} else {
							const primaryContact = primaryContacts[0];

							const consolidatedContact = {
								primaryContactId: primaryContact.id,
								emails: emails.filter(Boolean),
								phoneNumbers: phoneNumbers.filter(Boolean),
								secondaryContactIds: secondaryContacts,
							};

							if (
								(email &&
									!existingContacts.some(
										(contact) => contact.email === email
									)) ||
								(phoneNumber &&
									!existingContacts.some(
										(contact) =>
											contact.phoneNumber === phoneNumber
									))
							) {
								// Create a new secondary contact
								db.run(
									`INSERT INTO Contact (phoneNumber, email, linkedId, linkPrecedence) VALUES (?, ?, ?, 'secondary')`,
									[
										phoneNumber || null,
										email || null,
										primaryContact.id,
									],
									function (err) {
										if (err) {
											console.error("Error:", err);
											return res.status(500).json({
												error: "Internal Server Error",
											});
										}

										const secondaryContactId = this.lastID;

										console.log(
											"Create Secondary Contact Response:",
											JSON.stringify(consolidatedContact)
										);
										res.json({
											contact: {
												primaryContactId:
													primaryContact.id,
												emails: emails
													.concat(
														email !==
															primaryContact.email
															? email
															: null
													)
													.filter(Boolean),
												phoneNumbers: phoneNumbers
													.concat(
														phoneNumber !==
															primaryContact.phoneNumber
															? phoneNumber
															: null
													)
													.filter(Boolean),
												secondaryContactIds: [
													...consolidatedContact.secondaryContactIds,
													secondaryContactId,
												],
											},
										});
									}
								);
							} else {
								console.log(
									"Query response: ",
									JSON.stringify(consolidatedContact)
								);
								res.json({
									contact: consolidatedContact,
								});
							}
						}
					} else {
						// Create a new primary contact
						db.run(
							`INSERT INTO Contact (phoneNumber, email, linkedId, linkPrecedence) VALUES (?, ?, NULL, 'primary')`,
							[phoneNumber || null, email || null],
							function (err) {
								if (err) {
									console.error("Error:", err);
									return res.status(500).json({
										error: "Internal Server Error",
									});
								}

								const primaryContactId = this.lastID;

								const response = {
									primaryContactId,
									emails: [email].filter(Boolean),
									phoneNumbers: [phoneNumber].filter(Boolean),
									secondaryContactIds: [],
								};
								console.log(
									"Primary Contact Created: ",
									JSON.stringify({ contact: response })
								);
								res.json({ contact: response });
							}
						);
					}
				}
			);
		});
	} catch (err) {
		console.error("Error:", err);
		res.status(500).json({ error: "Internal Server Error" });
	}
});

// /get endpoint
app.get("/get", (req, res) => {
	const sql = `SELECT * FROM Contact`;

	db.all(sql, (err, rows) => {
		if (err) {
			console.error(err);
			return res.sendStatus(500);
		}
		console.log(JSON.stringify({ Contacts: rows }));
		res.json({ Contacts: rows });
	});
});

server.listen(port, () => {
	console.log(`Started on port ${port}`);
});
