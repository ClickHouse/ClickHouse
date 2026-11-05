-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: Explain output may differ

-- A column exploded by ARRAY JOIN can carry the name of the column the table partition key reads
-- (`ARRAY JOIN arr` keeps the name `arr`; under the old analyzer any alias keeps its user-facing name).
-- The per-partition optimizations must not treat such a key as the partition key's source column: the
-- same exploded value appears in arrays of different lengths, i.e. in many partitions, so per-partition
-- streams are not disjoint by it and skipping the merge or the scatter would produce wrong results.

-- The cost heuristics require enough balanced partitions relative to max_threads; the positive cases
-- below have 8 balanced partitions.
SET max_threads = 8;
-- The optimizations are disabled under parallel replicas.
SET enable_parallel_replicas = 0;

SET max_rows_to_sort = 0;
SET max_bytes_to_sort = 0;

-- Use the legacy EXPLAIN format so the assertions match plain marker lines without tree-drawing characters.
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_shadow;
CREATE TABLE t_shadow (arr Array(UInt32), v UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY length(arr);
-- Element 0 appears in arrays of every length, so it lives in all four partitions.
INSERT INTO t_shadow SELECT range(1 + number % 4), number FROM numbers(1000);

SET enable_analyzer = 0;

SELECT '-- old analyzer, window partitioned by the exploded column: must not engage even when forced';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT arr, sum(v) OVER (PARTITION BY arr ORDER BY v) FROM t_shadow ARRAY JOIN arr SETTINGS allow_window_partitions_independently = 1, force_window_partitions_independently = 1) WHERE explain LIKE '%Skip scatter%' OR explain LIKE '%separate port%';
SELECT (SELECT sum(cityHash64(arr, s)) FROM (SELECT arr, sum(v) OVER (PARTITION BY arr ORDER BY v) AS s FROM t_shadow ARRAY JOIN arr) SETTINGS allow_window_partitions_independently = 0) = (SELECT sum(cityHash64(arr, s)) FROM (SELECT arr, sum(v) OVER (PARTITION BY arr ORDER BY v) AS s FROM t_shadow ARRAY JOIN arr) SETTINGS allow_window_partitions_independently = 1, force_window_partitions_independently = 1);

SELECT '-- old analyzer, DISTINCT on the exploded column: must not engage even when forced';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT DISTINCT arr FROM t_shadow ARRAY JOIN arr SETTINGS allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1) WHERE explain LIKE '%separate port%' OR explain LIKE '%Skip stream merging%';
SELECT (SELECT count() FROM (SELECT DISTINCT arr FROM t_shadow ARRAY JOIN arr) SETTINGS allow_distinct_partitions_independently = 0) = (SELECT count() FROM (SELECT DISTINCT arr FROM t_shadow ARRAY JOIN arr) SETTINGS allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1);

SET enable_analyzer = 1;

SELECT '-- the analyzer: the exploded column gets a synthetic name and never matches; results stay equal';
SELECT (SELECT sum(cityHash64(arr, s)) FROM (SELECT arr, sum(v) OVER (PARTITION BY arr ORDER BY v) AS s FROM t_shadow ARRAY JOIN arr) SETTINGS allow_window_partitions_independently = 0) = (SELECT sum(cityHash64(arr, s)) FROM (SELECT arr, sum(v) OVER (PARTITION BY arr ORDER BY v) AS s FROM t_shadow ARRAY JOIN arr) SETTINGS allow_window_partitions_independently = 1, force_window_partitions_independently = 1);
SELECT (SELECT count() FROM (SELECT DISTINCT arr FROM t_shadow ARRAY JOIN arr) SETTINGS allow_distinct_partitions_independently = 0) = (SELECT count() FROM (SELECT DISTINCT arr FROM t_shadow ARRAY JOIN arr) SETTINGS allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1);

-- Keys that do not come from the exploded column must still be optimized across an ARRAY JOIN.
DROP TABLE IF EXISTS t_prec;
CREATE TABLE t_prec (k UInt32, arr Array(UInt32), v UInt32) ENGINE = MergeTree ORDER BY tuple() PARTITION BY k % 8;
INSERT INTO t_prec SELECT number % 32, [number % 3, number % 5], number FROM numbers(1000);

SELECT '-- window partitioned by a table column with an unrelated ARRAY JOIN: still engages';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT k, x, sum(v) OVER (PARTITION BY k ORDER BY v) FROM t_prec ARRAY JOIN arr AS x SETTINGS allow_window_partitions_independently = 1) WHERE explain LIKE '%Skip scatter%' OR explain LIKE '%separate port%';
SELECT (SELECT sum(cityHash64(k, x, s)) FROM (SELECT k, x, sum(v) OVER (PARTITION BY k ORDER BY v) AS s FROM t_prec ARRAY JOIN arr AS x) SETTINGS allow_window_partitions_independently = 0) = (SELECT sum(cityHash64(k, x, s)) FROM (SELECT k, x, sum(v) OVER (PARTITION BY k ORDER BY v) AS s FROM t_prec ARRAY JOIN arr AS x) SETTINGS allow_window_partitions_independently = 1);

SELECT '-- DISTINCT on a table column with an unrelated ARRAY JOIN: still engages';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT DISTINCT k FROM t_prec ARRAY JOIN arr SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT k FROM t_prec ARRAY JOIN arr) SETTINGS allow_distinct_partitions_independently = 0) = (SELECT count() FROM (SELECT DISTINCT k FROM t_prec ARRAY JOIN arr) SETTINGS allow_distinct_partitions_independently = 1);

SELECT '-- window partitioned by an ARRAY JOIN alias: not applicable, and the query works';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT x, sum(v) OVER (PARTITION BY x ORDER BY v) FROM t_prec ARRAY JOIN arr AS x SETTINGS allow_window_partitions_independently = 1, force_window_partitions_independently = 1) WHERE explain LIKE '%Skip scatter%' OR explain LIKE '%separate port%';
SELECT sum(cityHash64(x, s)) > 0 FROM (SELECT x, sum(v) OVER (PARTITION BY x ORDER BY v) AS s FROM t_prec ARRAY JOIN arr AS x);

-- An exploded column alongside a key column that determines the partition: equal key tuples imply an
-- equal table partition through the non-exploded column, so the optimization still applies.
SELECT '-- mixed keys (k, x) with an exploded x: still engages';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT k, x, sum(v) OVER (PARTITION BY k, x ORDER BY v) FROM t_prec ARRAY JOIN arr AS x SETTINGS allow_window_partitions_independently = 1) WHERE explain LIKE '%Skip scatter%' OR explain LIKE '%separate port%';
SELECT (SELECT sum(cityHash64(k, x, s)) FROM (SELECT k, x, sum(v) OVER (PARTITION BY k, x ORDER BY v) AS s FROM t_prec ARRAY JOIN arr AS x) SETTINGS allow_window_partitions_independently = 0) = (SELECT sum(cityHash64(k, x, s)) FROM (SELECT k, x, sum(v) OVER (PARTITION BY k, x ORDER BY v) AS s FROM t_prec ARRAY JOIN arr AS x) SETTINGS allow_window_partitions_independently = 1);
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT DISTINCT k, x FROM t_prec ARRAY JOIN arr AS x SETTINGS allow_distinct_partitions_independently = 1) WHERE explain LIKE '%separate port%';
SELECT (SELECT count() FROM (SELECT DISTINCT k, x FROM t_prec ARRAY JOIN arr AS x) SETTINGS allow_distinct_partitions_independently = 0) = (SELECT count() FROM (SELECT DISTINCT k, x FROM t_prec ARRAY JOIN arr AS x) SETTINGS allow_distinct_partitions_independently = 1);

SET enable_analyzer = 0;

SELECT '-- mixed keys, old analyzer: results stay equal';
SELECT (SELECT sum(cityHash64(k, x, s)) FROM (SELECT k, x, sum(v) OVER (PARTITION BY k, x ORDER BY v) AS s FROM t_prec ARRAY JOIN arr AS x) SETTINGS allow_window_partitions_independently = 0) = (SELECT sum(cityHash64(k, x, s)) FROM (SELECT k, x, sum(v) OVER (PARTITION BY k, x ORDER BY v) AS s FROM t_prec ARRAY JOIN arr AS x) SETTINGS allow_window_partitions_independently = 1);
SELECT (SELECT count() FROM (SELECT DISTINCT k, x FROM t_prec ARRAY JOIN arr AS x) SETTINGS allow_distinct_partitions_independently = 0) = (SELECT count() FROM (SELECT DISTINCT k, x FROM t_prec ARRAY JOIN arr AS x) SETTINGS allow_distinct_partitions_independently = 1);

DROP TABLE t_shadow;
DROP TABLE t_prec;
