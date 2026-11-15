-- Tags: atomic-database
-- `RENAME DATABASE` of the source of an incremental refreshable view is rejected:
-- a new table under the old name would be read from the view's stale cursor.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';

CREATE TABLE tgt (k UInt64) ENGINE = MergeTree ORDER BY k;

-- The view is in another database.
CREATE MATERIALIZED VIEW mv
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO tgt EMPTY
    AS SELECT k FROM {CLICKHOUSE_DATABASE_1:Identifier}.src;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (1);
SYSTEM REFRESH VIEW mv;
SYSTEM WAIT VIEW mv;

RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier}; -- { serverError HAVE_DEPENDENT_OBJECTS }

-- The source is intact and the view keeps reading it.
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (2);
SYSTEM REFRESH VIEW mv;
SYSTEM WAIT VIEW mv;
SELECT 'after_rejected_rename', groupArray(k) FROM (SELECT k FROM tgt ORDER BY k);

DROP TABLE mv;

-- The view is in the same database: its query keeps the old database name, so the rename is rejected too.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tgt (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO {CLICKHOUSE_DATABASE_1:Identifier}.tgt EMPTY
    AS SELECT k FROM {CLICKHOUSE_DATABASE_1:Identifier}.src;

RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier}; -- { serverError HAVE_DEPENDENT_OBJECTS }

-- Without the view the database can be renamed.
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.mv;
RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier};
SELECT 'renamed_without_view', count() FROM {CLICKHOUSE_DATABASE_2:Identifier}.src;

DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
DROP TABLE tgt;
