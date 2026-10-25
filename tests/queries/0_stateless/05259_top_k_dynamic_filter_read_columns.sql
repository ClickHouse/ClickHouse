-- Tags: no-parallel-replicas
-- The EXPLAIN controls assert on the local read step, which parallel replicas replace.

-- A top-N query (`ORDER BY ... LIMIT`) returns the same rows with and without `use_top_k_dynamic_filtering`
-- when the query is served by a projection that stores `_part_offset` and sorts by `_part_offset`,
-- and when the table has a column named like the threshold filter over the sort column.

SET use_skip_indexes_for_top_k = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET optimize_use_projections = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET enable_multiple_prewhere_read_steps = 1;

DROP TABLE IF EXISTS t_topk_part_offset;
DROP TABLE IF EXISTS t_topk_name_clash;
DROP TABLE IF EXISTS t_topk_no_clash;

CREATE TABLE t_topk_part_offset
(
    a Int32,
    b Int32,
    c Int32,
    PROJECTION p (SELECT a, b, c, _part_offset ORDER BY b)
)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

-- `b` and `c` are permutations of `a`, so no sort key has ties.
INSERT INTO t_topk_part_offset SELECT number, (number * 7919) % 100000, (number * 7907) % 100000 FROM numbers(100000);
-- One part, so `_part_offset` is unique.
OPTIMIZE TABLE t_topk_part_offset FINAL;

SELECT 'projection that stores _part_offset';

-- The projection serves the query.
SELECT count() > 0 FROM (
    EXPLAIN projections = 1
    SELECT _part_offset, a, b FROM t_topk_part_offset WHERE b < 5000 ORDER BY _part_offset LIMIT 3
    SETTINGS use_top_k_dynamic_filtering = 1)
WHERE explain LIKE '%ReadFromMergeTree (p)%';

SELECT _part_offset, a, b FROM t_topk_part_offset WHERE b < 5000 ORDER BY _part_offset LIMIT 3
SETTINGS use_top_k_dynamic_filtering = 1, query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000;
SELECT _part_offset, a, b FROM t_topk_part_offset WHERE b < 5000 ORDER BY _part_offset LIMIT 3
SETTINGS use_top_k_dynamic_filtering = 1, query_plan_optimize_lazy_materialization = 0;
SELECT _part_offset, a, b FROM t_topk_part_offset WHERE b < 5000 ORDER BY _part_offset LIMIT 3
SETTINGS use_top_k_dynamic_filtering = 0;

-- The same without a `WHERE` clause.
SELECT count() > 0 FROM (
    EXPLAIN projections = 1
    SELECT _part_offset, a, b FROM t_topk_part_offset ORDER BY _part_offset LIMIT 3
    SETTINGS use_top_k_dynamic_filtering = 1, prefer_optimize_projection = 1)
WHERE explain LIKE '%ReadFromMergeTree (p)%';

SELECT _part_offset, a, b FROM t_topk_part_offset ORDER BY _part_offset LIMIT 3
SETTINGS use_top_k_dynamic_filtering = 1, prefer_optimize_projection = 1;

-- Sorting the same projection by a column it stores under its own name keeps the threshold filter.
SELECT countIf(explain LIKE '%ReadFromMergeTree (p)%') > 0 AND countIf(explain LIKE '%\_\_topKFilter(c)%') > 0 FROM (
    EXPLAIN actions = 1
    SELECT _part_offset, c FROM t_topk_part_offset WHERE b < 5000 ORDER BY c LIMIT 3
    SETTINGS use_top_k_dynamic_filtering = 1);

SELECT 'column named like the threshold filter';

CREATE TABLE t_topk_name_clash (k UInt32, pred UInt32, `__topKFilter(k)` UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_topk_no_clash (k UInt32, pred UInt32, other UInt8) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_topk_name_clash SELECT number, number % 10, number * 37 % 251 FROM numbers(50000);
INSERT INTO t_topk_no_clash SELECT number, number % 10, number * 37 % 251 FROM numbers(50000);

SELECT k, `__topKFilter(k)` AS x FROM t_topk_name_clash PREWHERE pred = 3 ORDER BY k, x LIMIT 3
SETTINGS use_top_k_dynamic_filtering = 1;
SELECT k, `__topKFilter(k)` AS x FROM t_topk_name_clash WHERE pred = 3 ORDER BY k, x LIMIT 3
SETTINGS use_top_k_dynamic_filtering = 1;
SELECT k, `__topKFilter(k)` AS x FROM t_topk_name_clash WHERE pred = 3 ORDER BY k, x LIMIT 3
SETTINGS use_top_k_dynamic_filtering = 0;

-- The same query over a column with another name gets the threshold filter.
SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT k, other AS x FROM t_topk_no_clash PREWHERE pred = 3 ORDER BY k, x LIMIT 3
    SETTINGS use_top_k_dynamic_filtering = 1)
WHERE explain LIKE '%\_\_topKFilter(k)%';

DROP TABLE t_topk_part_offset;
DROP TABLE t_topk_name_clash;
DROP TABLE t_topk_no_clash;
