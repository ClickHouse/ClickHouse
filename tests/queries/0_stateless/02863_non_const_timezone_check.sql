DROP TABLE IF EXISTS Dates;

CREATE TABLE Dates (date DateTime('UTC')) ENGINE = MergeTree() ORDER BY date;

INSERT INTO Dates VALUES ('2023-08-25 15:30:00');

SELECT formatDateTime((SELECT date FROM Dates), '%H%i%S', number % 2 ? 'America/Los_Angeles' : 'Europe/Amsterdam') FROM numbers(5);

SELECT formatDateTime((SELECT materialize(date) FROM Dates), '%H%i%S', number % 2 ? 'America/Los_Angeles' : 'Europe/Amsterdam') FROM numbers(5);

SELECT formatDateTime((SELECT materialize(date) FROM Dates), '%H%i%S', number % 2 ? '' : 'Europe/Amsterdam') FROM numbers(5); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

SELECT toString((SELECT date FROM Dates), number % 2 ? 'America/Los_Angeles' : 'Europe/Amsterdam') FROM numbers(5);

SELECT toString((SELECT materialize(date) FROM Dates), number % 2 ? 'America/Los_Angeles' : 'Europe/Amsterdam') FROM numbers(5);

SELECT toString((SELECT materialize(date) FROM Dates), number % 2 ? 'America/Los_Angeles' : '') FROM numbers(5); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}

DROP TABLE Dates;
