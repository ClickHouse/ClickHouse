-- With `group_by_each_block_no_merge` every block is aggregated on its own. The trivial
-- `GROUP BY ... LIMIT` optimization shares one set of kept keys between all the blocks and
-- streams of the query, so the first block to reach the cutoff would decide which keys every
-- later block may emit. The optimization must stay off under `group_by_each_block_no_merge`:
-- neither the key cap (`OverflowAny`) nor the shared kept keys (`AggregationSharedKeptKeysRebuilds`)
-- may fire. The first query checks that the same query engages the optimization without the setting.
--
-- `enable_analyzer = 1` is pinned because the cutoff is armed by the planner of the analyzer.
-- `enable_parallel_replicas = 0` is pinned because the cutoff stays off on parallel replicas.
SET enable_analyzer = 1;
SET enable_group_by_top_k_optimization = 0;
SET optimize_trivial_group_by_limit_query = 1;
SET enable_parallel_replicas = 0;

SELECT toUInt64(number) AS k, count() AS c, sum(number) AS s FROM numbers_mt(1000000) GROUP BY k LIMIT 5 FORMAT Null
SETTINGS group_by_each_block_no_merge = 0, max_threads = 4, max_block_size = 8192, log_comment = '05361_merge';

SELECT toUInt64(number) AS k, count() AS c, sum(number) AS s FROM numbers_mt(1000000) GROUP BY k LIMIT 5 FORMAT Null
SETTINGS group_by_each_block_no_merge = 1, max_threads = 4, max_block_size = 8192, log_comment = '05361_no_merge';

-- Every block emits all of its own keys: here each block of 10 rows has keys `0..9`.
SELECT count(), sum(c) FROM
(
    SELECT number % 10 AS k, count() AS c FROM numbers(100) GROUP BY k LIMIT 1000
    SETTINGS group_by_each_block_no_merge = 1, max_threads = 1, max_block_size = 10
);

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['OverflowAny'] > 0 AS overflow_any_fired,
    ProfileEvents['AggregationSharedKeptKeysRebuilds'] > 0 AS kept_keys_rebuilds_fired
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment IN ('05361_merge', '05361_no_merge')
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
ORDER BY log_comment;
