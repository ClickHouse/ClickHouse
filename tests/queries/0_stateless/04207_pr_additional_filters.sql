-- Tags: no-parallel
-- ^ failpoint

SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS atf_p;
-- Pin index_granularity so EXPLAIN ... distributed=1 reports a stable granule count;
-- random index_granularity splits the 10 rows into >1 granule and breaks the reference.
CREATE TABLE atf_p (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
INSERT INTO atf_p SELECT number FROM numbers(10);

-- The failpoint disables cancellation of unused replicas after all ranges
-- are assigned, so every replica's contribution lands on the initiator and any
-- missing-filter regression is deterministic regardless of timing.

-- `additional_table_filters` keys are resolved against the initiator's session current
-- database, so they cannot be reliably matched on parallel-replica followers (the
-- rewritten query qualifies the table and the follower's current database differs).
-- On the legacy AST-forwarding path the analyzer rejects the combination instead of
-- silently dropping the filter.
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT count() FROM atf_p SETTINGS additional_table_filters = {'atf_p': 'x <= 2'},
    enable_analyzer = 1,
    enable_parallel_replicas = 2,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 1,
    serialize_query_plan = 0; -- { serverError SUPPORT_IS_DISABLED }

-- With `enable_parallel_replicas = 1` (best-effort) parallel replicas is silently
-- disabled and the query runs locally with the filter applied.
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT count() FROM atf_p SETTINGS additional_table_filters = {'atf_p': 'x <= 2'},
    enable_analyzer = 1,
    enable_parallel_replicas = 1,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 1,
    serialize_query_plan = 0;

-- With `serialize_query_plan = 1` the initiator lowers the additional filter into an
-- explicit `FilterStep` and ships the serialized plan, so the follower never
-- re-resolves the setting and the combination works.
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT count() FROM atf_p SETTINGS additional_table_filters = {'atf_p': 'x <= 2'},
    enable_analyzer = 1,
    enable_parallel_replicas = 2,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 1,
    serialize_query_plan = 1;

-- Verify the additional filter is present in the serialized plan shipped to followers.
-- Pinned to the query-based implementation: this asserts the exact plan text, and
-- `parallel_replicas_plan_based` builds a differently shaped (but equally filtered) fragment. The
-- queries above, which check that the filter is actually applied, run on the default implementation.
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
EXPLAIN PLAN actions = 1, distributed = 1
SELECT count() FROM atf_p SETTINGS additional_table_filters = {'atf_p': 'x <= 2'},
    enable_analyzer = 1,
    enable_parallel_replicas = 2,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    query_plan_remove_unused_columns = 1,
    parallel_replicas_local_plan = 1,
    serialize_query_plan = 1,
    distributed_aggregation_memory_efficient = 1, -- pin (randomized in CI): `MergingAggregated` prints its mode only when it is set
    parallel_replicas_plan_based = 0;

-- An `additional_table_filters` key may also be the table's alias, which analysis renames while
-- keeping the written form in `original_alias`; the planner must carry that across its query-tree
-- clones, or the filter is dropped (issue #118513). Strict mode makes a lost exemption an error.
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT count() FROM atf_p AS a SETTINGS additional_table_filters = {'a': 'x <= 2'},
    enable_analyzer = 1,
    enable_parallel_replicas = 2,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 1,
    serialize_query_plan = 1,
    parallel_replicas_plan_based = 0,
    parallel_replicas_prefer_local_replica = 1, -- pin: the local-plan exemption requires it
    log_comment = '04207_atf_alias_local_plan';

-- On the query-shipping path nothing is serialized: the followers get the rewritten `SELECT`, whose
-- table alias is the generated one, so an alias key cannot be resolved there at all. Parallel
-- replicas is disabled (best-effort) or rejected (strict) instead of dropping the filter.
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT count() FROM atf_p AS a SETTINGS additional_table_filters = {'a': 'x <= 2'},
    enable_analyzer = 1,
    enable_parallel_replicas = 1,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 0,
    serialize_query_plan = 1,
    parallel_replicas_plan_based = 0;

SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT count() FROM atf_p AS a SETTINGS additional_table_filters = {'a': 'x <= 2'},
    enable_analyzer = 1,
    enable_parallel_replicas = 2,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 0,
    serialize_query_plan = 1,
    parallel_replicas_plan_based = 0; -- { serverError SUPPORT_IS_DISABLED }

-- `parallel_replicas_plan_based` distributes a plan fragment that carries the filter on both of its
-- branches, so it keeps working without a local plan; strict mode turns a lost exemption into an
-- error instead of a silent fall back to local execution.
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT count() FROM atf_p AS a SETTINGS additional_table_filters = {'a': 'x <= 2'},
    enable_analyzer = 1,
    enable_parallel_replicas = 2,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 0,
    serialize_query_plan = 1,
    parallel_replicas_plan_based = 1,
    log_comment = '04207_atf_alias_plan_based';

-- A `database.table` key is matched by the storage's full name, which a follower replanning the
-- rewritten `SELECT` resolves the same way, so the filter still reaches every replica. The alias is
-- deliberately that same string: a full-name match settles the table whatever else the key names.
-- The database is named because a settings value is a literal, while this test's own one is random,
-- and the `system.one` entry matches no table of the query, so it must make no difference.
DROP DATABASE IF EXISTS db_04207_atf;
CREATE DATABASE db_04207_atf;
CREATE TABLE db_04207_atf.atf_q (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
INSERT INTO db_04207_atf.atf_q SELECT number FROM numbers(10);

SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT sum(x) FROM db_04207_atf.atf_q AS `db_04207_atf.atf_q`
SETTINGS additional_table_filters = {'system.one': 'dummy = 0', 'db_04207_atf.atf_q': 'x <= 2'},
    enable_analyzer = 1,
    enable_parallel_replicas = 2,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 0,
    serialize_query_plan = 1,
    parallel_replicas_plan_based = 0,
    log_comment = '04207_atf_qualified_key';

-- A bare table name is matched against the current database, and a follower's is the user's default
-- one, not the initiator's. It is refused whichever database is current here, because the two sides
-- would otherwise disagree about whether the filter applies at all.
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT sum(x) FROM db_04207_atf.atf_q AS a SETTINGS additional_table_filters = {'atf_q': 'x <= 2'},
    enable_analyzer = 1,
    enable_parallel_replicas = 2,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 0,
    serialize_query_plan = 1,
    parallel_replicas_plan_based = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP DATABASE db_04207_atf;

-- A quoted alias or table name may contain a dot itself, so the shape of the key decides nothing:
-- `a.b` is an alias here, resolvable only on the initiator, and is refused like any other alias.
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT count() FROM atf_p AS `a.b` SETTINGS additional_table_filters = {'a.b': 'x <= 2'},
    enable_analyzer = 1,
    enable_parallel_replicas = 2,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 0,
    serialize_query_plan = 1,
    parallel_replicas_plan_based = 0; -- { serverError SUPPORT_IS_DISABLED }

-- The generated aliases are not a stable namespace: the rewrite that ships a query builds its own
-- numbering, and can add relations to it, so a key in that namespace can be applied on a follower
-- and not here, adding a filter the same query never applies locally. Refused whether or not it
-- names anything in this query.
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT count() FROM atf_p AS a SETTINGS additional_table_filters = {'__table2': 'x <= 2'},
    enable_analyzer = 1,
    enable_parallel_replicas = 2,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 0,
    serialize_query_plan = 1,
    parallel_replicas_plan_based = 0; -- { serverError SUPPORT_IS_DISABLED }

-- Entries are matched against table functions as well, and their aliases are rewritten the same way,
-- so a key naming one is refused exactly like a table's alias.
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT sum(a.x) FROM atf_p AS a JOIN numbers(10) AS n ON a.x = n.number
SETTINGS additional_table_filters = {'n': 'number <= 2'},
    enable_analyzer = 1,
    enable_parallel_replicas = 2,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 0,
    serialize_query_plan = 1,
    parallel_replicas_plan_based = 0; -- { serverError SUPPORT_IS_DISABLED }

SYSTEM DISABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;

SYSTEM FLUSH LOGS query_log;

-- The scenarios above must really have run with parallel replicas. Strict mode only proves the
-- planner kept the exemption; execution can still decline it silently and apply the filter locally,
-- which prints the same count. Read the newest row per `log_comment`, so that a run which stops
-- engaging the replicas cannot be masked by an earlier one under a reused test database.
SELECT log_comment, argMax(ProfileEvents['ParallelReplicasUsedCount'], event_time_microseconds) > 0 AS pr_used
FROM system.query_log
WHERE event_date >= yesterday()
    AND type = 'QueryFinish'
    AND is_initial_query = 1
    AND current_database = currentDatabase()
    AND log_comment IN ('04207_atf_alias_local_plan', '04207_atf_alias_plan_based', '04207_atf_qualified_key')
GROUP BY log_comment
ORDER BY log_comment
SETTINGS enable_parallel_replicas = 0;

DROP TABLE atf_p;
