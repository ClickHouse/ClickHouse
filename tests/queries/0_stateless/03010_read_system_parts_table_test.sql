DROP TABLE IF EXISTS users;
CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=MergeTree ORDER BY uid PARTITION BY uid;

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);

SELECT uuid, name from system.parts WHERE table = 'users';

SELECT table from system.parts WHERE uuid = '00000000-0000-0000-0000-000000000000';
DROP TABLE IF EXISTS users;
