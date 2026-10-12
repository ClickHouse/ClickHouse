-- Automatic `LowCardinality` serialization through wrapper tables and across `RENAME COLUMN`.

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_auto_lc_buffer;
DROP TABLE IF EXISTS t_auto_lc_buffer_destination;
DROP TABLE IF EXISTS t_auto_lc_mv;
DROP TABLE IF EXISTS t_auto_lc_mv_target;
DROP TABLE IF EXISTS t_auto_lc_source;
DROP TABLE IF EXISTS t_auto_lc_rename;

CREATE TABLE t_auto_lc_source
(
    id UInt64,
    s String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 1000,
    ratio_of_defaults_for_sparse_serialization = 1,
    min_bytes_for_wide_part = 0,
    auto_statistics_types = 'basic';

INSERT INTO t_auto_lc_source SELECT number, 'v_' || toString(number % 10) FROM numbers(2000);

SELECT 'source kind';
SELECT DISTINCT serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_source' AND active AND column = 's';

-- `INSERT SELECT` from the encoded table keeps rows in the buffer, and a `Memory` destination cannot
-- encode anything, so the subcolumn rewrite applies and is resolved out of the buffered rows.
CREATE TABLE t_auto_lc_buffer_destination (id UInt64, s String) ENGINE = Memory;
CREATE TABLE t_auto_lc_buffer AS t_auto_lc_buffer_destination
ENGINE = Buffer(currentDatabase(), t_auto_lc_buffer_destination, 1, 1000, 1000, 1000000000, 1000000000, 1000000000, 1000000000);

INSERT INTO t_auto_lc_buffer SELECT id, s FROM t_auto_lc_source;

SELECT 'buffer';
SELECT sum(length(s)), countIf(notEmpty(s)), countIf(empty(s)) FROM t_auto_lc_buffer;
SELECT count() FROM t_auto_lc_buffer_destination;

-- A wrapper reads its target with a snapshot taken after the analysis, so the rewrite is not applied
-- to a `String` column of a `MergeTree` target, even when no part encodes it.
CREATE TABLE t_auto_lc_mv_target (id UInt64, s String) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW t_auto_lc_mv TO t_auto_lc_mv_target AS SELECT id, s FROM t_auto_lc_source;
INSERT INTO t_auto_lc_source SELECT number, 'w_' || toString(number % 10) FROM numbers(100);

SELECT 'materialized view: rewrite is skipped';
SELECT count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT length(s) FROM t_auto_lc_mv) WHERE explain LIKE '%s.size%';
SELECT sum(length(s)), countIf(notEmpty(s)) FROM t_auto_lc_mv;

SELECT 'direct read of the target: rewrite fires';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT length(s) FROM t_auto_lc_mv_target) WHERE explain LIKE '%s.size%';

-- A rewrite of a compact part after `RENAME COLUMN` keeps the encoding: the statistic of the renamed
-- column is looked up under its name in the source part.
CREATE TABLE t_auto_lc_rename
(
    id UInt64,
    lc String STATISTICS(uniq)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 1000,
    ratio_of_defaults_for_sparse_serialization = 1,
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000,
    auto_statistics_types = 'basic';

INSERT INTO t_auto_lc_rename SELECT number, 'v_' || toString(number % 10) FROM numbers(2000);

SELECT 'before rename';
SELECT part_type, column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_rename' AND active AND column LIKE 'lc%';

ALTER TABLE t_auto_lc_rename RENAME COLUMN lc TO lc2;

SELECT 'after rename';
SELECT part_type, column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_rename' AND active AND column LIKE 'lc%';
SELECT sum(length(lc2)), countIf(notEmpty(lc2)), uniqExact(lc2) FROM t_auto_lc_rename;

DROP TABLE t_auto_lc_buffer;
DROP TABLE t_auto_lc_buffer_destination;
DROP TABLE t_auto_lc_mv;
DROP TABLE t_auto_lc_mv_target;
DROP TABLE t_auto_lc_source;
DROP TABLE t_auto_lc_rename;
