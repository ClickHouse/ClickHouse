-- Tags: zookeeper, no-ordinary-database, no-fasttest

-- The test rewrites a shared `metadata` znode with `replaceOne`. The stress test profile enables the
-- server-side AST fuzzer for every query type, and a permuted argument list makes the replacement
-- literal the haystack, so the znode is left holding that bare literal instead of a full definition.
SET ast_fuzzer_runs = 0;
SET ast_fuzzer_any_query = 0;

DROP TABLE IF EXISTS 05024_keeper_map_parenthesized_metadata_bad SYNC;
DROP TABLE IF EXISTS 05024_keeper_map_parenthesized_metadata_malformed SYNC;
DROP TABLE IF EXISTS 05024_keeper_map_parenthesized_metadata_terminated SYNC;
DROP TABLE IF EXISTS 05024_keeper_map_parenthesized_metadata_second SYNC;
DROP TABLE IF EXISTS 05024_keeper_map_parenthesized_metadata_reverse SYNC;
DROP TABLE IF EXISTS 05024_keeper_map_parenthesized_metadata SYNC;
DROP TABLE IF EXISTS 05024_keeper_map_parenthesized_metadata_delimiter_second SYNC;
DROP TABLE IF EXISTS 05024_keeper_map_parenthesized_metadata_delimiter SYNC;

CREATE TABLE 05024_keeper_map_parenthesized_metadata (key UInt64, value String)
ENGINE = KeeperMap('/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata')
PRIMARY KEY key;

SELECT endsWith(value, 'primary key: key\n')
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata'
    AND name = 'metadata';

CREATE TABLE 05024_keeper_map_parenthesized_metadata_reverse (key UInt64, value String)
ENGINE = KeeperMap('/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata')
PRIMARY KEY(key);

SELECT endsWith(value, 'primary key: key\n')
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata'
    AND name = 'metadata';

INSERT INTO system.zookeeper (path, name, value)
SELECT path, name, replaceOne(value, 'primary key: key\n', 'primary key: (key)\n')
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata'
    AND name = 'metadata';

SELECT endsWith(value, 'primary key: (key)\n')
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata'
    AND name = 'metadata';

CREATE TABLE 05024_keeper_map_parenthesized_metadata_second
(
    key UInt64 COMMENT 'comment added later',
    value String
)
ENGINE = KeeperMap('/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata')
PRIMARY KEY key;

SELECT endsWith(value, 'primary key: (key)\n') AND position(value, 'comment added later') = 0
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata'
    AND name = 'metadata';

DETACH TABLE 05024_keeper_map_parenthesized_metadata;
ATTACH TABLE 05024_keeper_map_parenthesized_metadata;

SELECT endsWith(value, 'primary key: (key)\n')
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata'
    AND name = 'metadata';

INSERT INTO 05024_keeper_map_parenthesized_metadata VALUES (1, 'value');
SELECT * FROM 05024_keeper_map_parenthesized_metadata_second;

CREATE TABLE 05024_keeper_map_parenthesized_metadata_bad (key UInt64, value String)
ENGINE = KeeperMap('/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata')
PRIMARY KEY value; -- { serverError BAD_ARGUMENTS }

INSERT INTO system.zookeeper (path, name, value)
SELECT path, name, replaceOne(value, 'primary key: (key)\n', 'primary key: (key\n')
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata'
    AND name = 'metadata';

SELECT endsWith(value, 'primary key: (key\n')
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata'
    AND name = 'metadata';

CREATE TABLE 05024_keeper_map_parenthesized_metadata_malformed (key UInt64, value String)
ENGINE = KeeperMap('/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata')
PRIMARY KEY key; -- { serverError BAD_ARGUMENTS }

INSERT INTO system.zookeeper (path, name, value)
SELECT path, name, replaceOne(value, 'primary key: (key\n', 'primary key: key;\n')
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata'
    AND name = 'metadata';

SELECT endsWith(value, 'primary key: key;\n')
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata'
    AND name = 'metadata';

CREATE TABLE 05024_keeper_map_parenthesized_metadata_terminated (key UInt64, value String)
ENGINE = KeeperMap('/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata')
PRIMARY KEY key; -- { serverError BAD_ARGUMENTS }

INSERT INTO system.zookeeper (path, name, value)
SELECT path, name, replaceOne(value, 'primary key: key;\n', 'primary key: (key)\n')
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata'
    AND name = 'metadata';

CREATE TABLE 05024_keeper_map_parenthesized_metadata_delimiter (key UInt64, value String)
ENGINE = KeeperMap('/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata_delimiter')
PRIMARY KEY sipHash64(concat(toString(key), 'primary key: '));

CREATE TABLE 05024_keeper_map_parenthesized_metadata_delimiter_second
(
    key UInt64 COMMENT 'comment added later',
    value String
)
ENGINE = KeeperMap('/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata_delimiter')
PRIMARY KEY sipHash64(concat(toString(key), 'primary key: '));

DROP TABLE IF EXISTS 05024_keeper_map_parenthesized_metadata_bad SYNC;
DROP TABLE IF EXISTS 05024_keeper_map_parenthesized_metadata_malformed SYNC;
DROP TABLE IF EXISTS 05024_keeper_map_parenthesized_metadata_terminated SYNC;
DROP TABLE 05024_keeper_map_parenthesized_metadata_second SYNC;
DROP TABLE 05024_keeper_map_parenthesized_metadata_reverse SYNC;
DROP TABLE 05024_keeper_map_parenthesized_metadata SYNC;
DROP TABLE 05024_keeper_map_parenthesized_metadata_delimiter_second SYNC;
DROP TABLE 05024_keeper_map_parenthesized_metadata_delimiter SYNC;

-- A stored definition that cannot be parsed at all must leave the table loadable but unusable, so that
-- one unreadable path cannot stop the server from starting.
DROP TABLE IF EXISTS 05024_keeper_map_parenthesized_metadata_unparsable SYNC;
DROP TABLE IF EXISTS 05024_keeper_map_parenthesized_metadata_unparsable_second SYNC;

CREATE TABLE 05024_keeper_map_parenthesized_metadata_unparsable (key UInt64, value String)
ENGINE = KeeperMap('/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata_unparsable')
PRIMARY KEY key;

INSERT INTO 05024_keeper_map_parenthesized_metadata_unparsable VALUES (1, 'value');

INSERT INTO system.zookeeper (path, name, value)
SELECT '/test_keeper_map/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata_unparsable',
    'metadata', 'primary key: (key)\n';

SELECT value = 'primary key: (key)\n'
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata_unparsable'
    AND name = 'metadata';

DETACH TABLE 05024_keeper_map_parenthesized_metadata_unparsable;
ATTACH TABLE 05024_keeper_map_parenthesized_metadata_unparsable;

SELECT * FROM 05024_keeper_map_parenthesized_metadata_unparsable; -- { serverError INVALID_STATE }
INSERT INTO 05024_keeper_map_parenthesized_metadata_unparsable VALUES (2, 'value'); -- { serverError INVALID_STATE }

-- A single-key lookup is refused too, not only a whole-table read.
SELECT * FROM (SELECT toUInt64(1) AS key) AS probe
ANY LEFT JOIN 05024_keeper_map_parenthesized_metadata_unparsable USING key
SETTINGS join_algorithm = 'direct'; -- { serverError INVALID_STATE }

CREATE TABLE 05024_keeper_map_parenthesized_metadata_unparsable_second (key UInt64, value String)
ENGINE = KeeperMap('/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata_unparsable')
PRIMARY KEY key; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

-- Dropping a table whose metadata is invalid removes only the local table, so restore the definition
-- from the data node (which holds the same string) before dropping, or the path is left behind.
INSERT INTO system.zookeeper (path, name, value)
SELECT '/test_keeper_map/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata_unparsable',
    'metadata', value
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata_unparsable'
    AND name = 'data';

SELECT uniqExact(value) = 1
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase() || '/05024_keeper_map_parenthesized_metadata_unparsable'
    AND name IN ('metadata', 'data');

DETACH TABLE 05024_keeper_map_parenthesized_metadata_unparsable;
ATTACH TABLE 05024_keeper_map_parenthesized_metadata_unparsable;

SELECT * FROM 05024_keeper_map_parenthesized_metadata_unparsable;

DROP TABLE 05024_keeper_map_parenthesized_metadata_unparsable SYNC;

SELECT count()
FROM system.zookeeper
WHERE path = '/test_keeper_map/' || currentDatabase()
    AND name = '05024_keeper_map_parenthesized_metadata_unparsable';
