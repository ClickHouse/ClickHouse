-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: Explain output may differ

SET max_threads = 8;
-- The optimization is disabled under parallel replicas.
SET enable_parallel_replicas = 0;

-- { echo }

DROP TABLE IF EXISTS t;
CREATE TABLE t (a UInt32, b UInt32, arr Array(UInt32)) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES t;
INSERT INTO t SELECT number, number, range(number % 5) FROM numbers_mt(1e3);
INSERT INTO t SELECT number, number, range(number % 5) FROM numbers_mt(1e3);

DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY a % 8;
SYSTEM STOP MERGES t2;
INSERT INTO t2 SELECT number, number FROM numbers_mt(1e3);

-- POSITIVE: LIMIT BY over ARRAY JOIN
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a FROM t ARRAY JOIN arr LIMIT 1 BY a SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT a FROM t ARRAY JOIN arr LIMIT 1 BY a SETTINGS allow_limit_by_partitions_independently = 0)) = (SELECT count() FROM (SELECT a FROM t ARRAY JOIN arr LIMIT 1 BY a SETTINGS allow_limit_by_partitions_independently = 1));

-- POSITIVE: LIMIT BY above DISTINCT (LIMIT BY skips via propagation; its own request walk is blocked by the distinct steps)
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a, b FROM t LIMIT 1 BY a SETTINGS allow_distinct_partitions_independently = 1, allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT a, b FROM t LIMIT 1 BY a SETTINGS allow_distinct_partitions_independently = 0, allow_limit_by_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT a, b FROM t LIMIT 1 BY a SETTINGS allow_distinct_partitions_independently = 1, allow_limit_by_partitions_independently = 1));

-- POSITIVE: GROUP BY above DISTINCT (aggregation skips via propagation)
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, count() FROM (SELECT DISTINCT a, b FROM t) GROUP BY a SETTINGS allow_distinct_partitions_independently = 1, allow_aggregate_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT a, count() FROM (SELECT DISTINCT a, b FROM t) GROUP BY a SETTINGS allow_distinct_partitions_independently = 0, allow_aggregate_partitions_independently = 0)) = (SELECT count() FROM (SELECT a, count() FROM (SELECT DISTINCT a, b FROM t) GROUP BY a SETTINGS allow_distinct_partitions_independently = 1, allow_aggregate_partitions_independently = 1));

-- NEGATIVE: a non-allowlisted step (LIMIT, multi-port transform) between the reading and DISTINCT
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM (SELECT a FROM t LIMIT 100000) SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%Read each partition through separate port%';

-- NEGATIVE: a consumer above GROUP BY does not skip (aggregation rewrites columns -> barrier upward),
-- even though the inner aggregation itself skips merging
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM (SELECT a, count() FROM t GROUP BY a) SETTINGS allow_distinct_partitions_independently = 1, allow_aggregate_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%Read each partition through separate port%';

-- NEGATIVE: GROUP BY directly over ARRAY JOIN is not optimized (aggregation's request walk does not cross ARRAY JOIN)
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, count() FROM t ARRAY JOIN arr GROUP BY a SETTINGS allow_aggregate_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%Read each partition through separate port%';

-- POSITIVE: DISTINCT above LIMIT BY (LIMIT BY skips and preserves disjointness upward to the DISTINCT)
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM (SELECT a FROM t LIMIT 5 BY a) SETTINGS allow_distinct_partitions_independently = 1, allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT a FROM (SELECT a FROM t LIMIT 5 BY a) SETTINGS allow_distinct_partitions_independently = 0, allow_limit_by_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT a FROM (SELECT a FROM t LIMIT 5 BY a) SETTINGS allow_distinct_partitions_independently = 1, allow_limit_by_partitions_independently = 1));

-- POSITIVE: GROUP BY above LIMIT BY
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a, count() FROM (SELECT a FROM t LIMIT 5 BY a) GROUP BY a SETTINGS allow_limit_by_partitions_independently = 1, allow_aggregate_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT a, count() FROM (SELECT a FROM t LIMIT 5 BY a) GROUP BY a SETTINGS allow_limit_by_partitions_independently = 0, allow_aggregate_partitions_independently = 0)) = (SELECT count() FROM (SELECT a, count() FROM (SELECT a FROM t LIMIT 5 BY a) GROUP BY a SETTINGS allow_limit_by_partitions_independently = 1, allow_aggregate_partitions_independently = 1));

-- POSITIVE: DISTINCT above DISTINCT
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM (SELECT DISTINCT a, b FROM t) SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT a FROM (SELECT DISTINCT a, b FROM t) SETTINGS allow_distinct_partitions_independently = 0)) = (SELECT count() FROM (SELECT DISTINCT a FROM (SELECT DISTINCT a, b FROM t) SETTINGS allow_distinct_partitions_independently = 1));

-- POSITIVE: LIMIT BY above LIMIT BY
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a FROM (SELECT a, b FROM t LIMIT 5 BY a, b) LIMIT 1 BY a SETTINGS allow_limit_by_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%Read each partition through separate port%';
SELECT (SELECT count() FROM (SELECT a FROM (SELECT a, b FROM t LIMIT 5 BY a, b) LIMIT 1 BY a SETTINGS allow_limit_by_partitions_independently = 0)) = (SELECT count() FROM (SELECT a FROM (SELECT a, b FROM t LIMIT 5 BY a, b) LIMIT 1 BY a SETTINGS allow_limit_by_partitions_independently = 1));

-- NEGATIVE: LIMIT BY above GROUP BY (aggregation is an upward barrier; the inner aggregation still skips)
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT a FROM (SELECT a, count() c FROM t GROUP BY a) LIMIT 1 BY a SETTINGS allow_limit_by_partitions_independently = 1, allow_aggregate_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%Read each partition through separate port%';

-- NEGATIVE: DISTINCT over UNION ALL (multi-child barrier; the final DISTINCT does not skip)
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT a FROM (SELECT a FROM t UNION ALL SELECT a FROM t2) SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%Read each partition through separate port%';

-- NEGATIVE: DISTINCT after JOIN (multi-child barrier)
SELECT replaceRegexpOne(explain, '^[ ]*(.*)', '\\1') FROM (EXPLAIN actions = 1 SELECT DISTINCT t.a FROM t JOIN t2 ON t.a = t2.a SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%Skip stream merging%' OR explain LIKE '%Skip merging: 1%' OR explain LIKE '%Read each partition through separate port%';

DROP TABLE t2;
DROP TABLE t;
