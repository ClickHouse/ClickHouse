-- The `IgnoreSet` variants of `IN` return 0 for every row without reading their right operand (the
-- null-skipping variants still propagate a NULL left operand), and stay usable in the places that
-- inspect the query plan rather than execute it: aggregation hash table statistics, EXPLAIN, and the
-- filter analysis done for virtual columns and PREWHERE.

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

-- The plan holds the call with its left operand alone. A node carrying a second operand would carry
-- a set that no one built, which is what every plan consumer then reads.
SELECT 'explain, one operand';
SELECT count() > 0 FROM (
    EXPLAIN actions = 1 SELECT count() FROM numbers(10) GROUP BY inIgnoreSet(number, (1, 2, 3))
) WHERE explain ILIKE '%Keys: inIgnoreSet(number)%'
SETTINGS collect_hash_table_stats_during_aggregation = 1, explain_query_plan_default = 'pretty';

-- Resolving the call from the left operand alone must give the same result type and the same values
-- as resolving it from both operands, for every wrapper the operand can carry. The `if(...)` operand
-- supplies a NULL row, which the null-skipping and null-comparing variants treat differently.
SELECT 'nullable, null-skipping';
SELECT toTypeName(inIgnoreSet(if(number = 0, NULL, number), (1))) AS t,
       inIgnoreSet(if(number = 0, NULL, number), (1)) AS v, count()
FROM numbers(4) GROUP BY t, v ORDER BY t, v
SETTINGS collect_hash_table_stats_during_aggregation = 1;

SELECT 'nullable, null-comparing';
SELECT toTypeName(nullInIgnoreSet(if(number = 0, NULL, number), (1))) AS t,
       nullInIgnoreSet(if(number = 0, NULL, number), (1)) AS v, count()
FROM numbers(4) GROUP BY t, v ORDER BY t, v
SETTINGS collect_hash_table_stats_during_aggregation = 1;

SELECT 'low cardinality';
SELECT toTypeName(inIgnoreSet(toLowCardinality(number), (1))) AS t,
       inIgnoreSet(toLowCardinality(number), (1)) AS v, count()
FROM numbers(4) GROUP BY t, v ORDER BY t, v
SETTINGS collect_hash_table_stats_during_aggregation = 1;

SELECT 'low cardinality nullable';
SELECT toTypeName(inIgnoreSet(toLowCardinality(if(number = 0, NULL, number)), (1))) AS t,
       inIgnoreSet(toLowCardinality(if(number = 0, NULL, number)), (1)) AS v, count()
FROM numbers(4) GROUP BY t, v ORDER BY t, v
SETTINGS collect_hash_table_stats_during_aggregation = 1;

-- A filter pushed to a remote shard is rebuilt as SQL text out of the plan's expression DAG, where a
-- call with no set operand cannot be written back as `IN` syntax. Dropping that conjunct from the
-- text only widens what the shard returns; the initiator still applies the whole filter.
SELECT 'remote pushdown';
SELECT count() FROM (SELECT * FROM remote('127.0.0.1', currentDatabase(), t_in_ignore_set)) WHERE x > 7
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;
SELECT count() FROM (SELECT * FROM remote('127.0.0.1', currentDatabase(), t_in_ignore_set)) WHERE inIgnoreSet(x, (1, 2))
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;
SELECT count() FROM (SELECT * FROM remote('127.0.0.1', currentDatabase(), t_in_ignore_set)) WHERE globalInIgnoreSet(x, (1, 2))
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;
SELECT count() FROM (SELECT * FROM remote('127.0.0.1', currentDatabase(), t_in_ignore_set)) WHERE x > 7 AND inIgnoreSet(x, (1, 2))
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;

-- Without this the four arms above would pass on a build that never pushed anything down: the shard's
-- own plan carries its own copy of the convertible conjunct.
SELECT 'remote pushdown is reachable';
SELECT countIf(explain ILIKE '%Filter column: greater(%') > 1 FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT count() FROM (SELECT * FROM remote('127.0.0.1', currentDatabase(), t_in_ignore_set)) WHERE x > 7
)
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;

-- Declining the whole predicate whenever it contains an `IgnoreSet` call would pass every arm above
-- while silently stopping pushdown for such filters. When only that conjunct is dropped, the
-- initiator and the shard each render an `and(greater(...))` filter, so the count is 2, not 1.
SELECT 'remote pushdown keeps the convertible conjunct';
SELECT countIf(explain ILIKE '%Filter column: and(greater(%') > 1 FROM (
    EXPLAIN actions = 1, distributed = 1
    SELECT count() FROM (SELECT * FROM remote('127.0.0.1', currentDatabase(), t_in_ignore_set))
    WHERE x > 7 AND inIgnoreSet(x, (1, 2))
)
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;

-- A plan shipped to a shard is serialized, and the receiver rebuilds each function node from the
-- children it actually has, rejecting a node whose stored result type disagrees. Every read at
-- `serialize_query_plan = 1` is paired with the same read at 0, so each value is an oracle.
SELECT 'serialized plan round trip';
SELECT count() FROM remote('127.0.0.1', currentDatabase(), t_in_ignore_set)
WHERE inIgnoreSet(toLowCardinality(x), (1, 2))
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 1;
SELECT count() FROM remote('127.0.0.1', currentDatabase(), t_in_ignore_set)
WHERE inIgnoreSet(toLowCardinality(x), (1, 2))
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;
SELECT count() FROM remote('127.0.0.1', currentDatabase(), t_in_ignore_set)
WHERE inIgnoreSet(if(x = 0, NULL, x), (1, 2))
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 1;
SELECT count() FROM remote('127.0.0.1', currentDatabase(), t_in_ignore_set)
WHERE inIgnoreSet(if(x = 0, NULL, x), (1, 2))
SETTINGS prefer_localhost_replica = 0, serialize_query_plan = 0;

DROP TABLE t_in_ignore_set;
