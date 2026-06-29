DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS users_ext;
CREATE TABLE users (uid Int16, name String, age Int16) ENGINE = Memory;
INSERT INTO users VALUES (1231, 'John', 33);

CREATE table users_ext(uid Int16, nullableStringCol Nullable(String)) ENGINE = Memory;
INSERT INTO users_ext VALUES (123, NULL);

SELECT nullableStringCol IS NOT NULL FROM users LEFT JOIN users_ext ON users_ext.uid = users.uid;
SELECT count(nullableStringCol) FROM users LEFT JOIN users_ext ON users_ext.uid = users.uid;
SELECT nullableStringCol IS NULL FROM users LEFT JOIN users_ext ON users_ext.uid = users.uid;

SELECT nullableStringCol IS NULL FROM users JOIN users users2 ON users.uid = users2.uid LEFT JOIN users_ext ON users_ext.uid = users.uid;
SELECT nullableStringCol IS NULL FROM users_ext RIGHT JOIN users users2 ON users_ext.uid = users2.uid LEFT JOIN users ON users2.uid = users.uid;

SELECT nullableStringCol IS NULL FROM users_ext JOIN users users2 ON users_ext.uid = users2.uid FULL JOIN users ON users2.uid = users.uid;

SELECT t1.nullableStringCol IS NULL, t2.nullableStringCol IS NULL, t3.nullableStringCol IS NULL
FROM users_ext t1
FULL JOIN users_ext t2 ON t1.uid = t2.uid
FULL JOIN users_ext t3 ON t1.uid = t3.uid;
