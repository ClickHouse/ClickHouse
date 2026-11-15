CREATE TABLE users (date DateTime, name String, age Int16) ENGINE=MergeTree() ORDER BY date;

INSERT INTO users VALUES ('2024-01-01', 'John', 33),
                         ('2024-02-01', 'Ksenia', 48),
                         ('2024-02-15', 'Alice', 50);

SELECT * FROM users ORDER BY date WITH FILL TO '2024-02-17' STEP toIntervalHour(1); -- { serverError INVALID_WITH_FILL_EXPRESSION }
