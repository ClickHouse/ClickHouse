-- A filter that keeps only listed constants (`x = c`, `x IN (c1, ...)`) proves how many distinct values
-- `x` has, and a `DISTINCT` or `GROUP BY` on it has at most that many rows. Without the proof the bound of
-- such a subquery was the rows of its filtered read. The subqueries have no estimate here (no statistics,
-- and a filter on a non-key column), so the join label shows their bound: `d[no_stats<=N]`. Only that
-- label is checked: the rest of the plan varies with settings the runner randomizes.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET use_statistics = 0;
SET param__internal_join_table_stat_hints = '{"t_dc_outer": {"cardinality": 1000, "distinct_keys": {"k": 1000}}}';
-- The join inputs below list only the columns the join needs; the runner randomizes the removal.
SET query_plan_remove_unused_columns = 1;
SET optimize_move_to_prewhere = 1;
-- The plan lines below show one read port per read; the runner randomizes per-partition aggregation.
SET allow_aggregate_partitions_independently = 0;

DROP TABLE IF EXISTS t_dc_outer;
DROP TABLE IF EXISTS t_dc_values;
DROP TABLE IF EXISTS t_dc_decimal;

-- Sparse serialization changes the bytes a read costs, and the granule count is in the plan; the
-- runner randomizes both.
CREATE TABLE t_dc_outer (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1, index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE t_dc_values (id UInt64, v UInt64, w UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1, index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_dc_outer SELECT number FROM numbers(1000);
INSERT INTO t_dc_values SELECT number, number % 50, number % 7 FROM numbers(10000);
DROP TABLE IF EXISTS t_dc_decimal;
CREATE TABLE t_dc_decimal (id UInt64, d Decimal(38, 0)) ENGINE = MergeTree ORDER BY id
    SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1, index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_dc_decimal SELECT number, number % 50 FROM numbers(10000);
DROP TABLE IF EXISTS t_dc_strings;
CREATE TABLE t_dc_strings (id UInt64, s String) ENGINE = MergeTree ORDER BY id
    SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1, index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_dc_strings SELECT number, toString(number) FROM numbers(10000);
-- The runner randomizes the insert block size; one part per table keeps the granule counts in the plan fixed.
OPTIMIZE TABLE t_dc_outer FINAL;
OPTIMIZE TABLE t_dc_values FINAL;
OPTIMIZE TABLE t_dc_decimal FINAL;
OPTIMIZE TABLE t_dc_strings FINAL;

SELECT '-- DISTINCT over v IN (three values): at most 3 rows';
SELECT trimBoth(explain) FROM (EXPLAIN SELECT count() FROM t_dc_outer AS a
JOIN (SELECT DISTINCT v FROM t_dc_values WHERE v IN (1, 2, 3)) AS d ON a.k = d.v) WHERE explain LIKE '%⋈%';

SELECT '-- GROUP BY v, w with v = 5 and w IN (two values): at most 2 rows';
SELECT trimBoth(explain) FROM (EXPLAIN SELECT count() FROM t_dc_outer AS a
JOIN (SELECT v, w FROM t_dc_values WHERE v = 5 AND w IN (1, 2) GROUP BY v, w) AS d ON a.k = d.w) WHERE explain LIKE '%⋈%';

SELECT '-- DISTINCT over a UNION ALL of three and two values: at most 5 rows';
SELECT trimBoth(explain) FROM (EXPLAIN SELECT count() FROM t_dc_outer AS a
JOIN (SELECT DISTINCT v FROM (SELECT v FROM t_dc_values WHERE v IN (1, 2, 3) UNION ALL SELECT v FROM t_dc_values WHERE v IN (7, 8))) AS d
ON a.k = d.v) WHERE explain LIKE '%⋈%';

SELECT '-- NOT IN proves nothing: the bound stays the rows of the read';
SELECT trimBoth(explain) FROM (EXPLAIN SELECT count() FROM t_dc_outer AS a
JOIN (SELECT DISTINCT v FROM t_dc_values WHERE v NOT IN (1, 2, 3)) AS d ON a.k = d.v) WHERE explain LIKE '%⋈%';

SELECT '-- GROUPING SETS fills a key missing from a set with a default, so v = 5 proves no single value: the bound is the rows of each set';
SELECT trimBoth(explain) FROM (EXPLAIN SELECT count() FROM t_dc_outer AS a
JOIN (SELECT v FROM t_dc_values WHERE v = 5 GROUP BY GROUPING SETS ((v), ())) AS d ON a.k = d.v) WHERE explain LIKE '%⋈%';

SELECT '-- a Decimal column compared with a Float64 constant: several values can compare equal, no proof';
SELECT trimBoth(explain) FROM (EXPLAIN SELECT count() FROM t_dc_outer AS a
JOIN (SELECT DISTINCT d FROM t_dc_decimal WHERE d = 5.) AS d ON a.k = d.d) WHERE explain LIKE '%⋈%';

SELECT '-- a set from a subquery: no proof';
SELECT trimBoth(explain) FROM (EXPLAIN SELECT count() FROM t_dc_outer AS a
JOIN (SELECT DISTINCT v FROM t_dc_values WHERE v IN (SELECT number FROM numbers(3))) AS d ON a.k = d.v) WHERE explain LIKE '%⋈%';

SELECT '-- a projection alias is visible in WHERE: the filter is on substring(substring(s, 1, 2), 1, 2), which is the alias itself, at most 3 rows';
SELECT trimBoth(explain) FROM (EXPLAIN SELECT count() FROM t_dc_outer AS a
JOIN (SELECT DISTINCT p FROM (SELECT substr(s, 1, 2) AS p FROM t_dc_strings WHERE substr(p, 1, 2) IN ('10', '11', '12'))) AS d
ON toString(a.k) = d.p) WHERE explain LIKE '%⋈%';

SELECT '-- a shorter prefix of the alias keeps several values of it: no proof';
SELECT trimBoth(explain) FROM (EXPLAIN SELECT count() FROM t_dc_outer AS a
JOIN (SELECT DISTINCT p FROM (SELECT substr(s, 1, 2) AS p FROM t_dc_strings WHERE substr(p, 1, 1) IN ('1', '2', '3'))) AS d
ON toString(a.k) = d.p) WHERE explain LIKE '%⋈%';

DROP TABLE t_dc_strings;
DROP TABLE t_dc_outer;
DROP TABLE t_dc_values;
DROP TABLE t_dc_decimal;
