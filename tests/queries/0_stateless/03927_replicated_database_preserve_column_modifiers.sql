-- Tags: zookeeper, no-replicated-database, no-ordinary-database
-- no-replicated-database: this test explicitly creates a Replicated database.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};

-- [Setup begin] Global settings used by test sections below.
SET data_type_default_nullable = 1;
SET compatibility_ignore_collation_in_create_table = 1;
SET compatibility_ignore_auto_increment_in_create_table = 1;
-- [Setup end]

-- [Test setup begin] Create replicated database and switch to it.
CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier}
ENGINE = Replicated('/clickhouse/databases/{database}', 'shard1', 'replica1')
FORMAT Null;

USE {CLICKHOUSE_DATABASE:Identifier};
-- [Test setup end]

-- [Test 1 begin] Preserve explicit NOT NULL under data_type_default_nullable=1.
CREATE TABLE t_not_null
(
    key Int64 NOT NULL,
    value String
)
ENGINE = Memory
FORMAT Null;

SELECT name, type
FROM system.columns
WHERE database = currentDatabase() AND table = 't_not_null'
ORDER BY position;
-- [Test 1 end]

-- [Test 2 begin] Preserve COLLATE and NOT NULL in normalized CREATE query.
CREATE TABLE t_collation
(
    x varchar(255) COLLATE utf8_unicode_ci NOT NULL
)
ENGINE = Memory
FORMAT Null;

SELECT
    positionCaseInsensitive(create_table_query, 'COLLATE utf8_unicode_ci') > 0 AS has_collation,
    positionCaseInsensitive(create_table_query, 'NOT NULL') > 0 AS has_not_null
FROM system.tables
WHERE database = currentDatabase() AND name = 't_collation';
-- [Test 2 end]

-- [Test 3 begin] Preserve compatibility AUTO_INCREMENT marker in normalized CREATE query.
CREATE TABLE t_auto_increment
(
    x Int32 AUTO_INCREMENT
)
ENGINE = Memory
FORMAT Null;

SELECT
    positionCaseInsensitive(create_table_query, 'AUTO_INCREMENT') > 0 AS has_auto_increment
FROM system.tables
WHERE database = currentDatabase() AND name = 't_auto_increment';
-- [Test 3 end]

-- [Cleanup begin]
DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};
-- [Cleanup end]
