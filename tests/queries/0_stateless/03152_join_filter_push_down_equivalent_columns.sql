SET enable_analyzer = 1;

DROP TABLE IF EXISTS users;
CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=MergeTree order by (uid, name);

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);
INSERT INTO users VALUES (8888, 'Alice', 50);

DROP TABLE IF EXISTS users2;
CREATE TABLE users2 (uid Int16, name String, age2 Int16) ENGINE=MergeTree order by (uid, name);

INSERT INTO users2 VALUES (1231, 'John', 33);
INSERT INTO users2 VALUES (6666, 'Ksenia', 48);
INSERT INTO users2 VALUES (8888, 'Alice', 50);

-- { echoOn }

EXPLAIN header = 1, indexes = 1
SELECT name FROM users INNER JOIN users2 USING name WHERE users.name ='Alice';

SELECT '--';

EXPLAIN header = 1, indexes = 1
SELECT name FROM users LEFT JOIN users2 USING name WHERE users.name ='Alice';

SELECT '--';

EXPLAIN header = 1, indexes = 1
SELECT name FROM users RIGHT JOIN users2 USING name WHERE users2.name ='Alice';

-- { echoOff }

DROP TABLE users;
DROP TABLE users2;
