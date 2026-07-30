-- Tags: no-parallel-replicas
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/50593
-- Custom key parallel replicas must still merge per-replica partial results on the initiator
-- when the custom key is not a function of the GROUP BY keys (e.g. plain count()).
-- Previously distributed_group_by_no_merge=2 was set unconditionally, so count() returned
-- one partial row per replica instead of the merged total.

DROP TABLE IF EXISTS t_04545 SYNC;

CREATE TABLE t_04545 (number Int64, y UInt32) ENGINE = MergeTree ORDER BY number;
INSERT INTO t_04545 SELECT number, number % 3 FROM numbers(100000);

SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_mode = 'custom_key_sampling';
SET automatic_parallel_replicas_mode = 0;

-- Plain count(): custom key is NOT a GROUP BY key -> must be merged -> single row.
-- The log_comment lets the assertion at the end confirm that the replicas really ran, so these cases
-- cannot pass by parallel replicas being silently disabled for queries without GROUP BY.
SELECT 'count analyzer=1';
SELECT count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
SETTINGS parallel_replicas_custom_key = 'sipHash64(number)', enable_analyzer = 1,
    log_comment = '04545_plain_count_an1';

SELECT 'count analyzer=0';
SELECT count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
SETTINGS parallel_replicas_custom_key = 'sipHash64(number)', enable_analyzer = 0,
    log_comment = '04545_plain_count_an0';

-- The issue reports the same wrong result for the range mode, which builds a different filter, so the
-- merge has to be kept there too.
SELECT 'count range mode analyzer=1';
SELECT count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
SETTINGS parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = 'y',
    enable_analyzer = 1, log_comment = '04545_plain_count_range_an1';

SELECT 'count range mode analyzer=0';
SELECT count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
SETTINGS parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = 'y',
    enable_analyzer = 0, log_comment = '04545_plain_count_range_an0';

-- count() wrapped in a subquery (exact shape from the issue) -> single row.
SELECT 'count subquery';
SELECT * FROM (SELECT count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545))
SETTINGS parallel_replicas_custom_key = 'sipHash64(number)';

-- sum() forces a materialization (different aggregate path than count()); custom key is NOT a
-- GROUP BY key -> per-replica partials must be merged on the initiator -> single total row.
SELECT 'sum analyzer=1';
SELECT sum(number) FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
SETTINGS parallel_replicas_custom_key = 'sipHash64(number)', enable_analyzer = 1;

SELECT 'sum analyzer=0';
SELECT sum(number) FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
SETTINGS parallel_replicas_custom_key = 'sipHash64(number)', enable_analyzer = 0;

-- GROUP BY on a key that does NOT cover the custom key -> must be merged.
SELECT 'group by not covering custom key';
SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY y ORDER BY y
SETTINGS parallel_replicas_custom_key = 'sipHash64(number)';

-- GROUP BY covers the custom key (custom key is a function of the GROUP BY key) -> correct with or
-- without merge; results must still be the merged totals.
SELECT 'group by covering custom key (y)';
SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY y ORDER BY y
SETTINGS parallel_replicas_custom_key = 'y', enable_analyzer = 1;

SELECT 'group by covering custom key cityHash64(y)';
SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY y ORDER BY y
SETTINGS parallel_replicas_custom_key = 'cityHash64(y)', enable_analyzer = 1;

-- Non-deterministic custom key (references only the GROUP BY column y, but rand() scatters rows of
-- the same group across replicas) -> must NOT skip the merge -> single merged row per group.
-- Assert the number of output rows is 3 (merged), not one partial row per replica.
SELECT 'non-deterministic custom key merged rows analyzer=1';
SELECT count() FROM (
    SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
    GROUP BY y SETTINGS parallel_replicas_custom_key = 'y + rand()', enable_analyzer = 1
) SETTINGS enable_analyzer = 1;

SELECT 'non-deterministic custom key merged rows analyzer=0';
SELECT count() FROM (
    SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
    GROUP BY y SETTINGS parallel_replicas_custom_key = 'y + rand()', enable_analyzer = 0
) SETTINGS enable_analyzer = 0;

-- A custom key whose value can differ per replica for the same group must not take the no-merge fast
-- path even when it is deterministic in scope of query and not stateful. Row counts cannot detect this
-- here: all three "replicas" of the test cluster are the same process, so `getMacro`/`hostName` return
-- the same value on each and no scattering happens. Assert the decision itself instead, by reading
-- `distributed_group_by_no_merge` off the secondary queries: unset means the merge was kept, 2 means
-- the fast path was taken. Each probe is tagged with a `log_comment` that is forwarded to the replicas.
SELECT 'no_merge on secondary queries per custom key';
SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY y ORDER BY y
SETTINGS parallel_replicas_custom_key = 'sipHash64(getMacro(''replica''), y)', enable_analyzer = 1,
    log_comment = '04545_server_constant_an1';

SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY y ORDER BY y
SETTINGS parallel_replicas_custom_key = 'sipHash64(getMacro(''replica''), y)', enable_analyzer = 0,
    log_comment = '04545_server_constant_an0';

-- `hostName()` reaches the same rejection through `FunctionConstantBase`, unlike `getMacro` which
-- overrides the flags itself.
SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY y ORDER BY y
SETTINGS parallel_replicas_custom_key = 'sipHash64(hostName(), y)', enable_analyzer = 1,
    log_comment = '04545_server_constant_hostname_an1';

-- `timeSeriesStoreTags` declares isDeterministic() == true and is excluded only by isStateful(), so it
-- pins that the safety check is a conjunction of the flags and not a determinism check alone.
SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY y ORDER BY y
SETTINGS parallel_replicas_custom_key = 'timeSeriesStoreTags(toUInt64(y), [], ''k'', toString(y))', enable_analyzer = 1,
    log_comment = '04545_stateful_deterministic_an1';

-- `timeSeriesTagsToGroup` assigns group ids in encounter order from a per-query collector, so the same
-- group can get different custom-key values on different replicas, and the merge must be kept.
-- Only the decision is asserted, and the result is discarded with `FORMAT Null`: the encounter order
-- also varies between threads within one replica, so with several parts the per-replica partials already
-- disagree before any merge happens. The rows this returns are therefore not stable, and asserting them
-- (or their count) would measure that instead of whether the merge was kept.
SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY y ORDER BY y
SETTINGS parallel_replicas_custom_key = 'timeSeriesTagsToGroup([], ''k'', toString(y))', enable_analyzer = 1,
    log_comment = '04545_stateful_tags_to_group_an1'
FORMAT Null;

SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY y ORDER BY y
SETTINGS parallel_replicas_custom_key = 'timeSeriesTagsToGroup([], ''k'', toString(y))', enable_analyzer = 0,
    log_comment = '04545_stateful_tags_to_group_an0'
FORMAT Null;

-- Grouping by the unsafe expression itself must not make it safe to partition on: the replica filter and
-- the grouping are evaluated separately on each replica, so one group still ends up spread over several
-- replicas. This reaches the check through the GROUP BY key equality shortcut rather than the recursion.
SELECT count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY sipHash64(getMacro('replica'), y) ORDER BY 1
SETTINGS parallel_replicas_custom_key = 'sipHash64(getMacro(''replica''), y)', enable_analyzer = 0,
    log_comment = '04545_server_constant_is_group_key_an0';

SELECT count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY sipHash64(getMacro('replica'), y) ORDER BY 1
SETTINGS parallel_replicas_custom_key = 'sipHash64(getMacro(''replica''), y)', enable_analyzer = 1,
    log_comment = '04545_server_constant_is_group_key_an1';

-- Controls: a custom key that IS a safe function of the GROUP BY key must still take the fast path, so a
-- regression that rejects everything is caught by the same assertion.
SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY y ORDER BY y
SETTINGS parallel_replicas_custom_key = 'y', enable_analyzer = 1,
    log_comment = '04545_safe_bare_key_an1';

SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY y ORDER BY y
SETTINGS parallel_replicas_custom_key = 'sipHash64(y)', enable_analyzer = 1,
    log_comment = '04545_safe_hash_key_an1';

-- The same controls on the old analyzer, so the AST overload cannot regress to always returning false
-- without failing this test: every other old-analyzer case here asserts a result that is unchanged when
-- merging stays enabled, which would not detect a dead fast path.
SELECT y, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY y ORDER BY y
SETTINGS parallel_replicas_custom_key = 'y', enable_analyzer = 0,
    log_comment = '04545_safe_bare_key_an0';

SELECT mod(number, 3) AS m, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY mod(number, 3) ORDER BY m
SETTINGS parallel_replicas_custom_key = 'mod(number, 3)', enable_analyzer = 0,
    log_comment = '04545_safe_expression_key_an0';

SYSTEM FLUSH LOGS query_log;

-- The secondary queries run with `current_database` = 'default', so they are scoped to this run through
-- the query ids of their own initiators (the queries that ran in `currentDatabase()`). That keeps the
-- assertion independent of any concurrent run of this test in another database.
-- The initiators are matched by exact `log_comment`, not by prefix: `clickhouse-test` passes
-- `--log_comment '<test basename>-<database>'`, so the queries above that set no `log_comment` of their
-- own still carry a value starting with this test's number, and a prefix match would report them as
-- extra rows.
-- `enable_parallel_replicas = 0` is required: `system.query_log` is MergeTree-backed, so with the
-- session's `parallel_replicas_for_non_replicated_merge_tree = 1` this query would itself be routed
-- through custom-key parallel replicas and fail, as the session sets no custom key of its own.
WITH ['04545_plain_count_an0', '04545_plain_count_an1', '04545_plain_count_range_an0',
      '04545_plain_count_range_an1', '04545_safe_bare_key_an0', '04545_safe_bare_key_an1',
      '04545_safe_expression_key_an0', '04545_safe_hash_key_an1', '04545_server_constant_an0',
      '04545_server_constant_an1', '04545_server_constant_hostname_an1',
      '04545_server_constant_is_group_key_an0', '04545_server_constant_is_group_key_an1',
      '04545_stateful_deterministic_an1', '04545_stateful_tags_to_group_an0',
      '04545_stateful_tags_to_group_an1'] AS probes,
(
    SELECT groupArray(query_id) FROM system.query_log
    WHERE current_database = currentDatabase() AND is_initial_query = 1 AND type = 'QueryFinish'
        AND has(probes, log_comment) AND event_date >= today() - 1
) AS initiators
SELECT log_comment AS probe, count() AS secondary_queries, Settings['distributed_group_by_no_merge'] AS no_merge
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query = 0 AND has(initiators, initial_query_id)
    AND event_date >= today() - 1
GROUP BY probe, no_merge ORDER BY probe
SETTINGS enable_parallel_replicas = 0;

-- Expression GROUP BY key with a custom key that is a deterministic function of that expression
-- (custom key equals the GROUP BY expression) -> safe to skip the merge; results must still be the
-- merged totals.
SELECT 'expression group by covering custom key mod(number,3) analyzer=1';
SELECT mod(number, 3) AS m, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY mod(number, 3) ORDER BY m
SETTINGS parallel_replicas_custom_key = 'mod(number, 3)', enable_analyzer = 1;

SELECT 'expression group by covering custom key mod(number,3) analyzer=0';
SELECT mod(number, 3) AS m, count() FROM cluster(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_04545)
GROUP BY mod(number, 3) ORDER BY m
SETTINGS parallel_replicas_custom_key = 'mod(number, 3)', enable_analyzer = 0;

DROP TABLE t_04545 SYNC;
