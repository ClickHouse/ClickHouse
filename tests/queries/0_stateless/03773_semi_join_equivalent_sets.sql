CREATE TABLE users (uid UInt64, name String, age Int16) ENGINE=MergeTree ORDER BY uid;

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);
INSERT INTO users VALUES (8888, 'Alice', 50);

EXPLAIN
SELECT * FROM users LEFT ANY JOIN (SELECT number FROM numbers(10)) as t2 ON users.uid = t2.number WHERE t2.number = 0;

EXPLAIN actions = 1
SELECT * FROM users LEFT SEMI JOIN (SELECT number FROM numbers(10)) as t2 ON users.uid = t2.number WHERE t2.number = 1;