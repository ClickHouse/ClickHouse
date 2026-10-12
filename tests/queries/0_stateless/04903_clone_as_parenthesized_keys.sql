DROP TABLE IF EXISTS clone_as_parenthesized_keys_source;
DROP TABLE IF EXISTS clone_as_parenthesized_keys_target;
DROP TABLE IF EXISTS clone_as_parenthesized_primary_source;
DROP TABLE IF EXISTS clone_as_parenthesized_primary_target;

CREATE TABLE clone_as_parenthesized_keys_source (a UInt64, b UInt64)
ENGINE = MergeTree
PARTITION BY (a)
ORDER BY tuple(a);
INSERT INTO clone_as_parenthesized_keys_source VALUES (1, 10);

CREATE TABLE clone_as_parenthesized_keys_target
CLONE AS clone_as_parenthesized_keys_source
ENGINE = MergeTree
PARTITION BY a
ORDER BY a;
SELECT * FROM clone_as_parenthesized_keys_target;

CREATE TABLE clone_as_parenthesized_primary_source (a UInt64, b UInt64)
ENGINE = MergeTree
PRIMARY KEY (a);
INSERT INTO clone_as_parenthesized_primary_source VALUES (2, 20);

CREATE TABLE clone_as_parenthesized_primary_target
CLONE AS clone_as_parenthesized_primary_source
ENGINE = MergeTree
PRIMARY KEY a;
SELECT * FROM clone_as_parenthesized_primary_target;

DROP TABLE clone_as_parenthesized_keys_source;
DROP TABLE clone_as_parenthesized_keys_target;
DROP TABLE clone_as_parenthesized_primary_source;
DROP TABLE clone_as_parenthesized_primary_target;
