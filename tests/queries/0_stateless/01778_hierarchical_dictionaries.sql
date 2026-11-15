-- Tags: no-parallel

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.hierarchy_source_table (id UInt64, parent_id UInt64) ENGINE = TinyLog;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.hierarchy_source_table VALUES (1, 0), (2, 1), (3, 1), (4, 2);

CREATE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.hierarchy_flat_dictionary
(
    id UInt64,
    parent_id UInt64 HIERARCHICAL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'hierarchy_source_table' DB currentDatabase()))
LAYOUT(FLAT())
LIFETIME(MIN 1 MAX 1000);

SELECT 'Flat dictionary';

SELECT 'Get hierarchy';
SELECT dictGetHierarchy('hierarchy_flat_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get is in hierarchy';
SELECT dictIsIn('hierarchy_flat_dictionary', number, number) FROM system.numbers LIMIT 6;
SELECT 'Get children';
SELECT dictGetChildren('hierarchy_flat_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get all descendants';
SELECT dictGetDescendants('hierarchy_flat_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get descendants at first level';
SELECT dictGetDescendants('hierarchy_flat_dictionary', number, 1) FROM system.numbers LIMIT 6;

DROP DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.hierarchy_flat_dictionary;

CREATE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.hierarchy_hashed_dictionary
(
    id UInt64,
    parent_id UInt64 HIERARCHICAL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'hierarchy_source_table' DB currentDatabase()))
LAYOUT(HASHED())
LIFETIME(MIN 1 MAX 1000);

SELECT 'Hashed dictionary';

SELECT 'Get hierarchy';
SELECT dictGetHierarchy('hierarchy_hashed_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get is in hierarchy';
SELECT dictIsIn('hierarchy_hashed_dictionary', number, number) FROM system.numbers LIMIT 6;
SELECT 'Get children';
SELECT dictGetChildren('hierarchy_hashed_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get all descendants';
SELECT dictGetDescendants('hierarchy_hashed_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get descendants at first level';
SELECT dictGetDescendants('hierarchy_hashed_dictionary', number, 1) FROM system.numbers LIMIT 6;

DROP DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.hierarchy_hashed_dictionary;

CREATE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.hierarchy_cache_dictionary
(
    id UInt64,
    parent_id UInt64 HIERARCHICAL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'hierarchy_source_table' DB currentDatabase()))
LAYOUT(CACHE(SIZE_IN_CELLS 10))
LIFETIME(MIN 1 MAX 1000);

SELECT 'Cache dictionary';

SELECT 'Get hierarchy';
SELECT dictGetHierarchy('hierarchy_cache_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get is in hierarchy';
SELECT dictIsIn('hierarchy_cache_dictionary', number, number) FROM system.numbers LIMIT 6;

DROP DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.hierarchy_cache_dictionary;

CREATE DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.hierarchy_direct_dictionary
(
    id UInt64,
    parent_id UInt64 HIERARCHICAL
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'hierarchy_source_table' DB currentDatabase()))
LAYOUT(DIRECT());

SELECT 'Direct dictionary';

SELECT 'Get hierarchy';
SELECT dictGetHierarchy('hierarchy_direct_dictionary', number) FROM system.numbers LIMIT 6;
SELECT 'Get is in hierarchy';
SELECT dictIsIn('hierarchy_direct_dictionary', number, number) FROM system.numbers LIMIT 6;

DROP DICTIONARY {CLICKHOUSE_DATABASE_1:Identifier}.hierarchy_direct_dictionary;

DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.hierarchy_source_table;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
