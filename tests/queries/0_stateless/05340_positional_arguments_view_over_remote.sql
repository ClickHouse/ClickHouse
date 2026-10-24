-- Tags: distributed

-- `ORDER BY ALL` expands to the whole projection, so the constant `3` reaches the shard query as a
-- bare literal. The local replica of a `remote` read inside a view, and a `Merge` child re-analyzed
-- from its AST, must keep it a constant: the shard query is printed from a resolved query tree.

DROP TABLE IF EXISTS t_pos;
DROP TABLE IF EXISTS t_pos_m1;
DROP TABLE IF EXISTS t_pos_d;
DROP TABLE IF EXISTS t_pos_merge;
DROP TABLE IF EXISTS t_pos_idx;
DROP TABLE IF EXISTS t_pos_buffer;
DROP TABLE IF EXISTS t_pos_buffer_dist;
DROP TABLE IF EXISTS t_pos_buffer_src;
DROP VIEW IF EXISTS v_pos;

CREATE TABLE t_pos (k Int64, src Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pos SELECT number, 4 - number FROM numbers(5);

SELECT 'order';
SELECT * FROM view(SELECT 3, k, src FROM remote('127.0.0.{1,2}', currentDatabase(), t_pos) ORDER BY ALL DESC)
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 1;

SELECT 'pruned projection';
SELECT k FROM view(SELECT 3, k, src FROM remote('127.0.0.{1,2}', currentDatabase(), t_pos) ORDER BY ALL DESC)
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 1;

SELECT 'distinct';
SELECT k, src FROM view(SELECT DISTINCT 3, k, src FROM remote('127.0.0.{1,2}', currentDatabase(), t_pos) ORDER BY ALL DESC)
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 1, optimize_distinct_in_order = 1;

SELECT 'constant out of projection range';
SELECT * FROM view(SELECT 5, k FROM remote('127.0.0.{1,2}', currentDatabase(), t_pos) ORDER BY ALL)
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 1;

-- The body of a view read through the local replica is still resolved positionally, also when it is inlined.
CREATE VIEW v_pos AS SELECT src % 2 AS s, count() AS c FROM t_pos GROUP BY 1;
SELECT 'view body';
SELECT s, c FROM view(SELECT s, c FROM remote('127.0.0.{1,2}', currentDatabase(), v_pos) ORDER BY s)
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 1;
SELECT 'inlined view body';
SELECT s, c FROM view(SELECT s, c FROM remote('127.0.0.{1,2}', currentDatabase(), v_pos) ORDER BY s)
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 1, analyzer_inline_views = 1;

-- So are the other queries the local replica analyzes: a table function argument and a row policy.
SELECT 'table function in a subquery with settings';
SELECT k FROM remote('127.0.0.1', currentDatabase(), t_pos)
WHERE k IN (SELECT k FROM view(SELECT number % 5 AS k, count() FROM numbers(10) GROUP BY 1) SETTINGS max_threads = 1) ORDER BY k
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 1;
CREATE ROW POLICY OR REPLACE rp_05340 ON v_pos USING s IN (SELECT number FROM numbers(2) ORDER BY 1 DESC LIMIT 1) TO ALL;
SELECT 'view row policy';
SELECT s, c FROM remote('127.0.0.1', currentDatabase(), v_pos)
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 1, analyzer_inline_views = 0;
SELECT 'inlined view row policy';
SELECT s, c FROM remote('127.0.0.1', currentDatabase(), v_pos)
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 1, analyzer_inline_views = 1;
DROP ROW POLICY rp_05340 ON v_pos;
-- And the hidden query of `obfuscate` (it keeps `Date` values as they are).
SELECT 'obfuscate order by';
SELECT * FROM remote('127.0.0.1', obfuscate(SELECT toDate(number + 1) AS d FROM numbers(3) ORDER BY 1 DESC LIMIT 1)) LIMIT 1
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 1, obfuscate_seed = '05340';
SELECT 'obfuscate group by';
SELECT count() FROM (SELECT * FROM remote('127.0.0.1', obfuscate(SELECT number % 2 AS k, count() AS c FROM numbers(4) GROUP BY 1)) LIMIT 4)
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 1, obfuscate_seed = '05340';
-- And the predicate of `mergeTreeAnalyzeIndexes`, also inside a view.
CREATE TABLE t_pos_idx (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_pos_idx SELECT number FROM numbers(4);
SELECT 'index analysis predicate';
SELECT ranges FROM remote('127.0.0.1', mergeTreeAnalyzeIndexes(currentDatabase(), t_pos_idx, k IN (SELECT number FROM numbers(4) ORDER BY 1 DESC LIMIT 1)))
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 1;
SELECT ranges FROM view(SELECT ranges FROM remote('127.0.0.1', mergeTreeAnalyzeIndexes(currentDatabase(), t_pos_idx, k IN (SELECT number FROM numbers(4) ORDER BY 1 DESC LIMIT 1))))
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 1;
DROP TABLE t_pos_idx;

CREATE TABLE t_pos_m1 (k Int64, src Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pos_m1 SELECT number, 4 - number FROM numbers(5);
CREATE TABLE t_pos_d AS t_pos ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_pos);
CREATE TABLE t_pos_merge AS t_pos ENGINE = Merge(currentDatabase(), '^(t_pos_m1|t_pos_d)$');

SELECT 'merge';
SELECT * FROM (SELECT 3, k, src FROM t_pos_merge ORDER BY ALL DESC)
SETTINGS enable_positional_arguments = 1;

SELECT 'merge constant out of projection range';
SELECT 5, k FROM t_pos_merge ORDER BY ALL
SETTINGS enable_positional_arguments = 1;

-- A `Buffer` over `Distributed` read through the local replica in a view: both the `Buffer` re-analysis and
-- the local plan of the `Distributed` read keep the constant.
CREATE TABLE t_pos_buffer_src (k UInt64) ENGINE = Memory;
CREATE TABLE t_pos_buffer_dist AS t_pos_buffer_src ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_pos_buffer_src);
CREATE TABLE t_pos_buffer AS t_pos_buffer_src ENGINE = Buffer(currentDatabase(), t_pos_buffer_dist, 1, 1000, 10000, 1000, 10000, 1000000, 10000000);
INSERT INTO t_pos_buffer VALUES (1);

SELECT 'buffer over distributed';
SELECT * FROM view(SELECT 5, k FROM remote('127.0.0.1', currentDatabase(), t_pos_buffer) ORDER BY ALL)
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 1;

-- Automatic parallel replicas re-analyzes a recursive CTE member from the AST printed from its query tree; the
-- candidate plan must be built for the case to test that.
SELECT 'recursive cte with automatic parallel replicas';
WITH RECURSIVE r AS ((SELECT 5 AS c, k FROM t_pos ORDER BY ALL LIMIT 1) UNION ALL (SELECT c, k + 1 FROM r WHERE k < 2))
SELECT * FROM r ORDER BY k
SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas',
    automatic_parallel_replicas_min_bytes_per_replica = 0, enable_positional_arguments = 1,
    automatic_parallel_replicas_remember_boundary = 0, log_comment = '05340_recursive_cte_auto_parallel_replicas';
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['AutomaticParallelReplicasProbePlansBuilt'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment = '05340_recursive_cte_auto_parallel_replicas';

DROP TABLE t_pos_buffer;
DROP TABLE t_pos_buffer_dist;
DROP TABLE t_pos_buffer_src;
DROP TABLE t_pos_merge;
DROP TABLE t_pos_d;
DROP TABLE t_pos_m1;
DROP VIEW v_pos;
DROP TABLE t_pos;
