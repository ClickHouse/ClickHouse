-- Tags: no-shared-merge-tree
-- The parts might named differently with SMT
DROP TABLE IF EXISTS users;
CREATE TABLE users (uid Int16, name String, age Int16) ENGINE=MergeTree ORDER BY uid PARTITION BY uid;

INSERT INTO users VALUES (1231, 'John', 33);
INSERT INTO users VALUES (6666, 'Ksenia', 48);

SELECT uuid, name from system.parts WHERE database = currentDatabase() AND table = 'users';

SELECT uuid, name, table from system.parts WHERE database = currentDatabase() AND table = 'users' AND uuid = '00000000-0000-0000-0000-000000000000';
SELECT uuid, name, table, column from system.parts_columns WHERE database = currentDatabase() AND table = 'users' AND uuid = '00000000-0000-0000-0000-000000000000';
DROP TABLE IF EXISTS users;
