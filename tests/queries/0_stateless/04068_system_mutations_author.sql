-- Tags: no-parallel, zookeeper

DROP TABLE IF EXISTS test_mutations_author_regular;
DROP TABLE IF EXISTS test_mutations_author_replicated;
DROP USER IF EXISTS test_mutations_author_user;
DROP USER IF EXISTS `test_mutations\\author_special`;

CREATE TABLE test_mutations_author_regular (id UInt64, value String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE test_mutations_author_replicated (id UInt64, value String) ENGINE = ReplicatedMergeTree ('/clickhouse/{database}/test_mutations_author_replicated', '1') ORDER BY id;
CREATE USER test_mutations_author_user IDENTIFIED WITH no_password;
GRANT ALTER UPDATE ON *.* TO test_mutations_author_user;
CREATE USER `test_mutations\\author_special` IDENTIFIED WITH no_password;
GRANT ALTER UPDATE ON *.* TO `test_mutations\\author_special`;

INSERT INTO test_mutations_author_regular VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO test_mutations_author_replicated VALUES (4, 'e'), (5, 'f'), (6, 'g');

SELECT name, type FROM system.columns WHERE database = 'system' AND table = 'mutations' AND name = 'author';

-- default user mutations
ALTER TABLE test_mutations_author_regular UPDATE value = 'x' WHERE id = 1 SETTINGS mutations_sync = 1;
ALTER TABLE test_mutations_author_replicated UPDATE value = 'y' WHERE id = 4 SETTINGS mutations_sync = 1;

SELECT database, table, is_done, author FROM system.mutations
WHERE database = currentDatabase() AND table IN ('test_mutations_author_regular', 'test_mutations_author_replicated')
ORDER BY table, mutation_id;

-- custom user mutation
EXECUTE AS test_mutations_author_user ALTER TABLE test_mutations_author_regular UPDATE value = 'z' WHERE id = 2 SETTINGS mutations_sync = 1;

SELECT database, table, is_done, author FROM system.mutations
WHERE database = currentDatabase() AND table = 'test_mutations_author_regular'
ORDER BY mutation_id;

-- user with special character (backslash) in name - tests escaping
EXECUTE AS `test_mutations\\author_special` ALTER TABLE test_mutations_author_regular UPDATE value = 'w' WHERE id = 3 SETTINGS mutations_sync = 1;
EXECUTE AS `test_mutations\\author_special` ALTER TABLE test_mutations_author_replicated UPDATE value = 'w' WHERE id = 5 SETTINGS mutations_sync = 1;

SELECT database, table, is_done, author FROM system.mutations
WHERE database = currentDatabase() AND table IN ('test_mutations_author_regular', 'test_mutations_author_replicated')
ORDER BY table, mutation_id;

-- reload mutations from disk
DETACH TABLE test_mutations_author_regular;
ATTACH TABLE test_mutations_author_regular;

-- reload mutations from ZooKeeper
DETACH TABLE test_mutations_author_replicated;
ATTACH TABLE test_mutations_author_replicated;

SELECT database, table, is_done, author FROM system.mutations
WHERE database = currentDatabase() AND table IN ('test_mutations_author_regular', 'test_mutations_author_replicated')
ORDER BY table, mutation_id;

DROP TABLE test_mutations_author_regular;
DROP TABLE test_mutations_author_replicated;
DROP USER test_mutations_author_user;
DROP USER `test_mutations\\author_special`;
