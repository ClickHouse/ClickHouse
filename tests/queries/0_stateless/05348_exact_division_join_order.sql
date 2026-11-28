CREATE TABLE t1 (a Nullable(Int64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE t2 (a Nullable(Int64), k Nullable(Float64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE t3 (k Nullable(Float64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
INSERT INTO t1 VALUES (1), (2);
INSERT INTO t2 VALUES (1, 10);
INSERT INTO t3 VALUES (5);
SET single_join_prefer_left_table = 0, join_use_nulls = 1;
-- The assertions compare the local join-plan shape.
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET query_plan_optimize_join_order_randomize = 0, use_hash_table_stats_for_join_reordering = 0;
SET query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_conflict_detector = 'a';
SET param__internal_join_table_stat_hints = '{"t1": {"cardinality": 100000, "distinct_keys": {"a": 2}}, "t2": {"cardinality": 1, "distinct_keys": {"a": 1, "k": 1}}, "t3": {"cardinality": 1, "distinct_keys": {"k": 1}}}';

SELECT 'baseline' AS arm, s AS step FROM (
    SELECT replaceRegexpOne(replaceRegexpOne(replaceRegexpOne(explain, '^[^A-Za-z]+', ''),
        '^Join \(.*$', 'Join'), '^ReadFromMergeTree \([^.]+\.([^)]+)\).*$', 'Read(\1)') AS s
    FROM (
        EXPLAIN SELECT count() FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t2.k / 2 = t3.k
        SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 0, sql_compatible_division_for_integers = 1
    )
) WHERE s IN ('Join', 'Read(t1)', 'Read(t2)', 'Read(t3)');

SELECT 'legacy' AS arm, s AS step FROM (
    SELECT replaceRegexpOne(replaceRegexpOne(replaceRegexpOne(explain, '^[^A-Za-z]+', ''),
        '^Join \(.*$', 'Join'), '^ReadFromMergeTree \([^.]+\.([^)]+)\).*$', 'Read(\1)') AS s
    FROM (
        EXPLAIN SELECT count() FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t2.k / 2 = t3.k
        SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10, sql_compatible_division_for_integers = 0
    )
) WHERE s IN ('Join', 'Read(t1)', 'Read(t2)', 'Read(t3)');

SELECT 'exact' AS arm, s AS step FROM (
    SELECT replaceRegexpOne(replaceRegexpOne(replaceRegexpOne(explain, '^[^A-Za-z]+', ''),
        '^Join \(.*$', 'Join'), '^ReadFromMergeTree \([^.]+\.([^)]+)\).*$', 'Read(\1)') AS s
    FROM (
        EXPLAIN SELECT count() FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t2.k / 2 = t3.k
        SETTINGS join_use_nulls = 1, query_plan_optimize_join_order_limit = 10, sql_compatible_division_for_integers = 1
    )
) WHERE s IN ('Join', 'Read(t1)', 'Read(t2)', 'Read(t3)');

SELECT count(), sum(ifNull(t2.k, -1)), sum(ifNull(t3.k, -1))
FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t2.k / 2 = t3.k
SETTINGS query_plan_optimize_join_order_limit = 10, sql_compatible_division_for_integers = 0;
SELECT count(), sum(ifNull(t2.k, -1)), sum(ifNull(t3.k, -1))
FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t2.k / 2 = t3.k
SETTINGS query_plan_optimize_join_order_limit = 10, sql_compatible_division_for_integers = 1;
DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
