-- `enable_join_runtime_filters_index_analysis` asks for a granule pruning that happens on data read,
-- driven by descriptors attached to the read step while a query plan is optimized.
--
-- This test pins which execution modes prune. Parallel replicas do: each replica prunes its own assigned
-- granules with its own filter, which is built from the whole build side (that side is broadcast, read in
-- full on every replica), so a granule it drops cannot hold a row that should have matched. That holds
-- however the replica got its plan - planning the query itself or optimizing one it deserialized - because
-- the descriptors are attached by an optimization that runs in both cases. A distributed query plan
-- (`make_distributed_plan = 1`) does not prune, which is the one no-op the setting's description still
-- claims.
--
-- Every mode must return exactly the result of a local read, which is what makes the pruning safe rather
-- than merely faster.

DROP TABLE IF EXISTS rf_idx_fact SYNC;
DROP TABLE IF EXISTS rf_idx_dim SYNC;

CREATE TABLE rf_idx_fact (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16;
CREATE TABLE rf_idx_dim (id UInt64, tag String) ENGINE = MergeTree ORDER BY id;
INSERT INTO rf_idx_fact SELECT number, number FROM numbers(2000);
INSERT INTO rf_idx_dim SELECT number, if(number < 64, 'hot', 'cold') FROM numbers(2000);

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes_on_data_read = 1;
SET join_runtime_filter_min_probe_rows = 0;
-- Which side builds the runtime filter decides whether the left side can be pruned at all, so the
-- randomized join-order perturbation has to be off.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET automatic_parallel_replicas_mode = 0;
-- Distributed aggregation cannot enforce a global limit, and the functional-test profile may set one.
SET max_rows_to_group_by = 0;
-- Every mode below is selected per query, so neither of the two rebuild paths may leak in from a profile.
SET make_distributed_plan = 0;
SET enable_parallel_replicas = 0;

-- Local read: the supported mode, kept here as the control that the workload really does prune.
SELECT 'local', count(), sum(f.v)
FROM rf_idx_fact AS f INNER JOIN rf_idx_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05153_local', enable_parallel_replicas = 0;

-- Distributed query plan: the left side is read by a rebuilt step inside a fragment.
SELECT 'distributed_plan', count(), sum(f.v)
FROM rf_idx_fact AS f INNER JOIN rf_idx_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05153_distributed_plan', enable_parallel_replicas = 0, make_distributed_plan = 1,
    distributed_plan_execute_locally = 1, distributed_plan_default_reader_bucket_count = 2,
    distributed_plan_default_shuffle_join_bucket_count = 2;

-- Parallel replicas, both the coordinator-driven reads and the plan-based ones.
SELECT 'parallel_replicas', count(), sum(f.v)
FROM rf_idx_fact AS f INNER JOIN rf_idx_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
-- `parallel_replicas_local_plan` is pinned so that the initiator really does read its own share here,
-- which is what makes `initiator_never_reads_unpruned` below say something about this mode.
SETTINGS log_comment = '05153_parallel_replicas', enable_parallel_replicas = 1, parallel_replicas_plan_based = 0,
    parallel_replicas_local_plan = 1;

-- Parallel replicas where the replicas receive a serialized plan instead of the query text: the
-- descriptors are not serialized with it, but the replica's own optimization of that plan attaches them.
SELECT 'parallel_replicas_serialized_plan', count(), sum(f.v)
FROM rf_idx_fact AS f INNER JOIN rf_idx_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05153_parallel_replicas_serialized_plan', enable_parallel_replicas = 1,
    parallel_replicas_plan_based = 0, serialize_query_plan = 1, parallel_replicas_local_plan = 1;

SELECT 'parallel_replicas_plan_based', count(), sum(f.v)
FROM rf_idx_fact AS f INNER JOIN rf_idx_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05153_parallel_replicas_plan_based', enable_parallel_replicas = 1,
    parallel_replicas_plan_based = 1, parallel_replicas_local_plan = 0;

-- Plan-based with a local plan: the initiator reads its own share through a cloned fragment. That fragment
-- is optimized with the runtime filters already in it, so the optimization that attaches the descriptors
-- does not run and cannot re-attach them - they have to survive the clone and the read-step rebuild. The
-- `initiator_never_reads_unpruned` column below is what pins it; a total over all replicas would be
-- satisfied by the remote replicas alone.
SELECT 'parallel_replicas_plan_based_local_plan', count(), sum(f.v)
FROM rf_idx_fact AS f INNER JOIN rf_idx_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05153_parallel_replicas_plan_based_local_plan', enable_parallel_replicas = 1,
    parallel_replicas_plan_based = 1, parallel_replicas_local_plan = 1;

SYSTEM FLUSH LOGS query_log;

-- A plan fragment arrives as a plan packet rather than SQL, so its `query_log` row carries neither the
-- test database nor the `log_comment`; it is reached through the initiator's `initial_query_id`.
SELECT
    initiator.log_comment AS mode,
    sum(part.ProfileEvents['RuntimeFilterGranulesConsidered']) > 0 AS granules_considered,
    sum(part.ProfileEvents['RuntimeFilterGranulesDropped']) > 0 AS granules_dropped,
    -- The initiator must never read marks locally without pruning them. Asserting that it *did* prune
    -- would be non-deterministic: with this few granules the coordinator can leave the initiator's share
    -- empty, and then there is nothing to consider. Before the descriptors were carried through the clone
    -- and the read-step rebuild, the plan-based local plan read every mark with this at 0.
    sumIf(part.ProfileEvents['RuntimeFilterGranulesConsidered'], part.is_initial_query) > 0
        OR sumIf(part.ProfileEvents['SelectedMarks'], part.is_initial_query) = 0 AS initiator_never_reads_unpruned
FROM system.query_log AS part
INNER JOIN
(
    SELECT query_id, log_comment
    FROM system.query_log
    WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish'
        AND log_comment IN ('05153_local', '05153_distributed_plan', '05153_parallel_replicas',
            '05153_parallel_replicas_serialized_plan', '05153_parallel_replicas_plan_based',
            '05153_parallel_replicas_plan_based_local_plan')
        AND event_date >= yesterday() AND event_time > now() - INTERVAL 1 HOUR
) AS initiator ON part.initial_query_id = initiator.query_id
WHERE part.type = 'QueryFinish' AND part.event_date >= yesterday() AND part.event_time > now() - INTERVAL 1 HOUR
GROUP BY mode
ORDER BY mode
SETTINGS enable_parallel_replicas = 0, enable_join_runtime_filters_index_analysis = 0;

DROP TABLE rf_idx_fact SYNC;
DROP TABLE rf_idx_dim SYNC;
