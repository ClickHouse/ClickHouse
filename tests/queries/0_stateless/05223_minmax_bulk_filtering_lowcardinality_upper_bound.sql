-- Tags: no-parallel-replicas
-- no-parallel-replicas: per-query SETTINGS toggling skip-index evaluation paths
-- must take effect on the executing replica.

-- Regression test: a `LowCardinality` index column made every comparison node of the bulk minmax
-- `ActionsDAG` return `LowCardinality(UInt8)`. The lower-bound float path happened to strip the wrapper
-- through the `NaN` handling, but upper bounds and equality did not, and reading `can_be_true` as a
-- plain `ColumnUInt8` threw a logical error.

SET allow_suspicious_low_cardinality_types = 1;
-- The test runner randomizes both; either one can move the queries off the bulk path.
SET secondary_indices_enable_bulk_filtering = 1;
SET use_skip_indexes_on_data_read = 0;
-- Otherwise the part-level statistics prune granules before the skip index, depending on the
-- randomized `materialize_statistics_on_insert`, and `EXPLAIN` shows an extra `Granules` line.
SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_bulk_lc_upper;
DROP TABLE IF EXISTS t_bulk_lc_int;

CREATE TABLE t_bulk_lc_upper
(
    f LowCardinality(Float64),
    INDEX idx_f f TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO t_bulk_lc_upper VALUES (1.0), (2.0), (3.0), (nan);
INSERT INTO t_bulk_lc_upper VALUES (-10.0), (-9.0), (-8.0), (-7.0);
INSERT INTO t_bulk_lc_upper VALUES (100.0), (200.0), (300.0), (400.0);

SELECT 'lc float upper bound', count() FROM t_bulk_lc_upper WHERE f <= 2.5 SETTINGS use_minmax_index_bulk_filtering = 1;
SELECT 'lc float strict upper bound', count() FROM t_bulk_lc_upper WHERE f < -8.0 SETTINGS use_minmax_index_bulk_filtering = 1;
SELECT 'lc float reversed operands', count() FROM t_bulk_lc_upper WHERE 9007199254740994. >= f SETTINGS use_minmax_index_bulk_filtering = 1;
SELECT 'lc float equals', count() FROM t_bulk_lc_upper WHERE f = 200.0 SETTINGS use_minmax_index_bulk_filtering = 1;
SELECT 'lc float not equals', count() FROM t_bulk_lc_upper WHERE f != 200.0 SETTINGS use_minmax_index_bulk_filtering = 1;
SELECT 'lc constant on the right', count() FROM t_bulk_lc_upper WHERE toLowCardinality(toUInt16(2)) >= f SETTINGS use_minmax_index_bulk_filtering = 1;

-- The bulk path must skip the same granules as the scalar path.
SELECT 'lc float upper bound parity',
    length(groupUniqArray(c)) = 1 AS all_equal,
    any(c) AS count
FROM
(
    SELECT count() AS c FROM t_bulk_lc_upper WHERE f <= 2.5 SETTINGS use_minmax_index_bulk_filtering = 0
    UNION ALL
    SELECT count() AS c FROM t_bulk_lc_upper WHERE f <= 2.5 SETTINGS use_minmax_index_bulk_filtering = 1
);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bulk_lc_upper WHERE f <= 2.5 SETTINGS use_minmax_index_bulk_filtering = 1)
WHERE explain LIKE '%Granules:%';

CREATE TABLE t_bulk_lc_int
(
    v LowCardinality(Int32),
    INDEX idx_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO t_bulk_lc_int VALUES (1), (2), (3), (4);
INSERT INTO t_bulk_lc_int VALUES (10), (20), (30), (40);

SELECT 'lc int upper bound', count() FROM t_bulk_lc_int WHERE v <= 3 SETTINGS use_minmax_index_bulk_filtering = 1;
SELECT 'lc int lower bound', count() FROM t_bulk_lc_int WHERE v > 25 SETTINGS use_minmax_index_bulk_filtering = 1;
SELECT 'lc int equals', count() FROM t_bulk_lc_int WHERE v = 30 SETTINGS use_minmax_index_bulk_filtering = 1;

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_bulk_lc_int WHERE v <= 3 SETTINGS use_minmax_index_bulk_filtering = 1)
WHERE explain LIKE '%Granules:%';

DROP TABLE t_bulk_lc_upper;
DROP TABLE t_bulk_lc_int;
