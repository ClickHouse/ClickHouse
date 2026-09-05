-- Automatic parallel replicas builds a second, throwaway plan to decide whether replicas pay off.
-- That probe plan must adopt the `IN` sets the single-node plan has already filled instead of
-- running their subqueries a second time.
--
-- `SetsBuiltFromSubquery` counts the sets filled by executing their subquery, so the query below
-- must report 1 rather than 2. The join matters: without it the probe plan never reaches the set
-- and the count is 1 either way, which would make this test pass even with the sharing removed.

DROP TABLE IF EXISTS t_autopr_sets;
DROP TABLE IF EXISTS t_autopr_sets_join;
DROP TABLE IF EXISTS t_autopr_sets_in;

CREATE TABLE t_autopr_sets (key UInt64, pad String) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_autopr_sets SELECT number, repeat('x', 20) FROM numbers(100000);

CREATE TABLE t_autopr_sets_join (key UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_autopr_sets_join SELECT number FROM numbers(20000);

CREATE TABLE t_autopr_sets_in (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_autopr_sets_in SELECT number * 7 FROM numbers(100);

-- Read the columns rather than wrapping the query in `count()`: an aggregate over the join lets the
-- probe plan be skipped, and then the set is never rebuilt for a reason unrelated to sharing.
SELECT t.key, t.pad
FROM t_autopr_sets AS t
JOIN t_autopr_sets_join AS u ON t.key = u.key
WHERE t.key IN (SELECT x FROM t_autopr_sets_in)
FORMAT Null
SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    enable_analyzer = 1, log_comment = '05042_autopr_share_prepared_sets';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['SetsBuiltFromSubquery']
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '05042_autopr_share_prepared_sets'
    AND type = 'QueryFinish'
    AND is_initial_query;

DROP TABLE t_autopr_sets;
DROP TABLE t_autopr_sets_join;
DROP TABLE t_autopr_sets_in;
