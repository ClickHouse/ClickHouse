CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=Memory;

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);
INSERT INTO users VALUES (8888, 'Alice', 50);

CREATE ROW POLICY a ON users FOR SELECT USING arrayExists(a -> a = age, [uid]) TO ALL;

SELECT * FROM users;
