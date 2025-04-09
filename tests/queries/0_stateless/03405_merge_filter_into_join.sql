CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=Memory;

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);
INSERT INTO users VALUES (8888, 'Alice', 50);

-- { echoOn }

EXPLAIN PLAN actions = 1
SELECT * FROM (SELECT * FROM users u1 INNER JOIN users u2 ON 1) WHERE age = u2.age ORDER BY ALL;
SELECT * FROM (SELECT * FROM users u1 INNER JOIN users u2 ON 1) WHERE age = u2.age ORDER BY ALL;

EXPLAIN PLAN actions = 1
SELECT * FROM (SELECT * FROM users u1 CROSS JOIN users u2) WHERE age = u2.age ORDER BY ALL;
SELECT * FROM (SELECT * FROM users u1 CROSS JOIN users u2) WHERE age = u2.age ORDER BY ALL;
