-- Tags: zookeeper, no-ordinary-database, no-fasttest

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
