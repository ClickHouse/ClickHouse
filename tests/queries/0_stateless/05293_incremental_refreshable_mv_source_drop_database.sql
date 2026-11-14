-- Tags: atomic-database
-- `DROP DATABASE` of the source of an incremental refreshable view in another database is rejected
-- before the source is prepared for shutdown, so its background merges keep working.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';

CREATE TABLE tgt (k UInt64) ENGINE = MergeTree ORDER BY k;

CREATE MATERIALIZED VIEW mv
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO tgt EMPTY
    AS SELECT k FROM {CLICKHOUSE_DATABASE_1:Identifier}.src;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (1);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (2);

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier}; -- { serverError HAVE_DEPENDENT_OBJECTS }
TRUNCATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier}; -- { serverError HAVE_DEPENDENT_OBJECTS }

-- The source and its rows survive the rejected drops.
SELECT 'source_rows', count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.src;

-- Merges were not cancelled by the rejected drops.
OPTIMIZE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src FINAL;
SELECT 'active_parts', uniqExact(_part) FROM {CLICKHOUSE_DATABASE_1:Identifier}.src;

-- The view keeps reading the source.
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (3);
SYSTEM REFRESH VIEW mv;
SYSTEM WAIT VIEW mv;
SELECT 'after_rejected_drop', groupArray(k) FROM (SELECT k FROM tgt ORDER BY k);

-- Without the view the database can be dropped.
DROP TABLE mv;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE tgt;
