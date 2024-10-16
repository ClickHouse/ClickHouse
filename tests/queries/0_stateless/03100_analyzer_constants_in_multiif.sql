-- https://github.com/ClickHouse/ClickHouse/issues/59101
SET enable_analyzer = 1;

CREATE TABLE users (name String, age Int16) ENGINE=Memory;
INSERT INTO users VALUES ('John', 33);
INSERT INTO users VALUES ('Ksenia', 48);
INSERT INTO users VALUES ('Alice', 50);

SELECT
    multiIf((age > 30) or (true), '1', '2') AS a,
    max(name)
FROM users
GROUP BY a;
