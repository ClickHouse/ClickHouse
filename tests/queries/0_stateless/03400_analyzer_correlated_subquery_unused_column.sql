set enable_analyzer = 1;

DROP TABLE IF EXISTS users;
CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=MergeTree() ORDER BY uid;

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);
INSERT INTO users VALUES (8888, 'Alice', 50);

DROP TABLE IF EXISTS users2;
CREATE TABLE users2 (uid Int16, name String, age Int16) ENGINE=MergeTree() ORDER BY uid;

INSERT INTO users2 VALUES (1231, 'John', 33);

-- { echoOn }

SET allow_experimental_correlated_subqueries = 1;

SELECT name FROM users u1
WHERE EXISTS (
  SELECT * FROM users2 u2
  WHERE u1.age = u2.age
);

SELECT name
FROM users AS u1
WHERE (age = 50) OR exists((
    SELECT *
    FROM users2 AS u2
    WHERE u1.age = u2.age
))
ORDER BY ALL
SETTINGS allow_experimental_correlated_subqueries = 1
