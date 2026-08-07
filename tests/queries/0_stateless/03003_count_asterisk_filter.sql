CREATE TABLE users (uid Int16, name Nullable(String), age Int16) ENGINE=Memory;

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, Null, 48);
INSERT INTO users VALUES (8888, 'Alice', 50);

SELECT count(name) FILTER (WHERE uid > 2000) FROM users;
SELECT countIf(name, uid > 2000) FROM users;

SELECT count(*) FILTER (WHERE uid > 2000) FROM users;
SELECT countIf(uid > 2000) FROM users;

DROP TABLE users;
