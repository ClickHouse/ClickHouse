-- The `IgnoreSet` variants of `IN` return 0 for every row without reading their right operand, and
-- stay usable in the places that inspect the query plan rather than execute it: aggregation
-- hash table statistics, EXPLAIN, and the filter analysis done for virtual columns and PREWHERE.

DROP TABLE IF EXISTS t_in_ignore_set;
CREATE TABLE t_in_ignore_set (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_in_ignore_set SELECT number FROM numbers(10);

SELECT 'aggregation';
SELECT inIgnoreSet(number, (1, 2, 3)) AS x, count() FROM numbers(10) GROUP BY x ORDER BY x
SETTINGS collect_hash_table_stats_during_aggregation = 1;

-- The negated variants return 0 as well, not the 1 that a `NOT IN` against an empty set would give.
SELECT 'aggregation, negated';
SELECT notInIgnoreSet(number, (1, 2, 3)) AS x, count() FROM numbers(10) GROUP BY x ORDER BY x
SETTINGS collect_hash_table_stats_during_aggregation = 1;

SELECT 'all eight variants';
SELECT
    inIgnoreSet(number, (1)) AS a,
    globalInIgnoreSet(number, (1)) AS b,
    notInIgnoreSet(number, (1)) AS c,
    globalNotInIgnoreSet(number, (1)) AS d,
    nullInIgnoreSet(number, (1)) AS e,
    globalNullInIgnoreSet(number, (1)) AS f,
    notNullInIgnoreSet(number, (1)) AS g,
    globalNotNullInIgnoreSet(number, (1)) AS h,
    count()
FROM numbers(4) GROUP BY a, b, c, d, e, f, g, h
SETTINGS collect_hash_table_stats_during_aggregation = 1;

SELECT 'result type';
SELECT toTypeName(inIgnoreSet(number, (1))) AS t, count() FROM numbers(4) GROUP BY t
SETTINGS collect_hash_table_stats_during_aggregation = 1;

SELECT 'explain';
SELECT count() > 0 FROM (
    EXPLAIN SELECT count() FROM numbers(10) GROUP BY inIgnoreSet(number, (1, 2, 3))
) WHERE explain ILIKE '%Aggregating%'
SETTINGS collect_hash_table_stats_during_aggregation = 1;

SELECT 'inside a lambda';
SELECT arrayMap(y -> inIgnoreSet(y, (1)), [1, 2]) AS a, count() FROM numbers(3) GROUP BY a
SETTINGS collect_hash_table_stats_during_aggregation = 1;

SELECT 'virtual column filter';
SELECT count() FROM system.tables WHERE database = currentDatabase() AND inIgnoreSet(name, ('no_such_table'));

SELECT 'prewhere';
SELECT count() FROM t_in_ignore_set PREWHERE inIgnoreSet(x, (1, 2)) WHERE x > 0;

-- A plain `IN` keeps reading its set, so these stay sensitive to the set contents.
SELECT 'plain IN';
SELECT number IN (1, 2, 3) AS x, count() FROM numbers(10) GROUP BY x ORDER BY x
SETTINGS collect_hash_table_stats_during_aggregation = 1;
SELECT number NOT IN (1, 2, 3) AS x, count() FROM numbers(10) GROUP BY x ORDER BY x
SETTINGS collect_hash_table_stats_during_aggregation = 1;
SELECT number FROM numbers(10) WHERE number IN (SELECT 1 UNION ALL SELECT 2) GROUP BY number ORDER BY number
SETTINGS collect_hash_table_stats_during_aggregation = 1;

DROP TABLE t_in_ignore_set;
