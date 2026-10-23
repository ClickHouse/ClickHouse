-- Tags: long, no-parallel-replicas
-- The staging asserted below happens inside one aggregating producer. With parallel replicas the
-- partial aggregation runs on the replicas, whose ProfileEvents never reach this server's
-- query_log, so the assertions would read zero.

DROP TABLE IF EXISTS t_adaptive_repl_left;
DROP TABLE IF EXISTS t_adaptive_repl_right;

-- The granularity settings decide how many rows a reader emits per block, and the join makes its
-- replication decision per block, so they are pinned here: the runner randomizes them and only
-- injects the ones a CREATE does not already set.
CREATE TABLE t_adaptive_repl_left (k UInt64, g UInt64, s String, u UInt128, nv Nullable(UInt64))
    ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
             ratio_of_defaults_for_sparse_serialization = 1.0, use_const_adaptive_granularity = 0,
             merge_max_block_size = 8192;
CREATE TABLE t_adaptive_repl_right (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0,
             ratio_of_defaults_for_sparse_serialization = 1.0, use_const_adaptive_granularity = 0,
             merge_max_block_size = 8192;

INSERT INTO t_adaptive_repl_left
SELECT number, number, concat('s_', toString(number)), toUInt128(number) * 7, if(number % 5 = 0, NULL, number * 3)
FROM numbers(400000);

-- One key window in eight matches three right rows instead of one. A block that grows keeps its
-- left columns lazily replicated while a block that does not is materialized, so the aggregation
-- receives a mix of replicated and dense argument columns at the same position.
INSERT INTO t_adaptive_repl_right SELECT number FROM numbers(400000);
INSERT INTO t_adaptive_repl_right SELECT number FROM numbers(400000) WHERE intDiv(number, 2048) % 8 = 0;
INSERT INTO t_adaptive_repl_right SELECT number FROM numbers(400000) WHERE intDiv(number, 2048) % 8 = 0;
OPTIMIZE TABLE t_adaptive_repl_right FINAL;

-- The staged batches are coalesced only while several of them are buffered together, so the two
-- external-aggregation thresholds have to stay off: either one drains the backlog one batch at a
-- time and the coalescing is skipped. A two-level conversion is incompatible with the frozen path,
-- and the size hint can divert the table before the freeze. All four are randomized by the test
-- runner, as is enable_lazy_columns_replication, which is what produces the mixed argument
-- representations in the first place.
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SET group_by_two_level_threshold = 10000000;
SET group_by_two_level_threshold_bytes = 500000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET enable_lazy_columns_replication = 1;
SET max_threads = 4;
SET max_block_size = 4096;
SET query_plan_join_swap_table = 0;
SET log_queries = 1;
SET log_profile_events = 1;

-- The mix also depends on where the join output blocks break and on how many right rows each
-- block carries, so the settings that reshape those blocks are pinned as well.
SET max_joined_block_size_rows = 65409;
SET joined_block_split_single_row = 0;
SET join_output_by_rowlist_perkey_rows_threshold = 5;
SET max_bytes_before_external_join = 0;
SET grace_hash_join_initial_buckets = 1;
SET query_plan_join_shard_by_pk_ranges = 0;
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 1000;

-- Each aggregate is computed twice, once through the adaptive aggregator and once through the
-- baseline one, so the pair also checks the coalesced values and not only the absence of an abort.

SELECT 'String';
SELECT sum(cityHash64(g, m)) FROM (
    SELECT l.g AS g, max(l.s) AS m FROM t_adaptive_repl_left AS l
    JOIN t_adaptive_repl_right AS r ON l.k = r.k GROUP BY l.g)
SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 0,
         log_comment = '04676_seal_string';
SELECT sum(cityHash64(g, m)) FROM (
    SELECT l.g AS g, max(l.s) AS m FROM t_adaptive_repl_left AS l
    JOIN t_adaptive_repl_right AS r ON l.k = r.k GROUP BY l.g)
SETTINGS enable_adaptive_aggregator = 0;

SELECT 'UInt128';
SELECT sum(cityHash64(g, m)) FROM (
    SELECT l.g AS g, max(l.u) AS m FROM t_adaptive_repl_left AS l
    JOIN t_adaptive_repl_right AS r ON l.k = r.k GROUP BY l.g)
SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 0,
         log_comment = '04676_seal_uint128';
SELECT sum(cityHash64(g, m)) FROM (
    SELECT l.g AS g, max(l.u) AS m FROM t_adaptive_repl_left AS l
    JOIN t_adaptive_repl_right AS r ON l.k = r.k GROUP BY l.g)
SETTINGS enable_adaptive_aggregator = 0;

SELECT 'Nullable(UInt64)';
SELECT sum(cityHash64(g, toString(m))) FROM (
    SELECT l.g AS g, max(l.nv) AS m FROM t_adaptive_repl_left AS l
    JOIN t_adaptive_repl_right AS r ON l.k = r.k GROUP BY l.g)
SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 0,
         log_comment = '04676_seal_nullable';
SELECT sum(cityHash64(g, toString(m))) FROM (
    SELECT l.g AS g, max(l.nv) AS m FROM t_adaptive_repl_left AS l
    JOIN t_adaptive_repl_right AS r ON l.k = r.k GROUP BY l.g)
SETTINGS enable_adaptive_aggregator = 0;

-- The coalescing above is entered only once at least two staged batches are buffered together;
-- a lone batch is published as it is, by an earlier return. The counter is incremented after that
-- return, so a non-zero count is what distinguishes the three arms having exercised the
-- coalescing from their having agreed on a value without ever reaching it.
SYSTEM FLUSH LOGS query_log;
SELECT 'sealed chunks', coalesce(sum(ProfileEvents['AdaptiveAggregationSealedChunks']), 0) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND event_date >= yesterday() AND event_time >= now() - 600
    AND log_comment LIKE '04676_seal_%';

-- The seal normalizes a gathered argument column that arrived in a wrapped representation, and
-- the join's lazily replicated blocks guarantee such columns reach the seal. The count is
-- summed over the three arms because which blocks stay replicated is the join's decision.
SELECT 'normalized', coalesce(sum(ProfileEvents['AdaptiveAggregationSealNormalizations']), 0) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND event_date >= yesterday() AND event_time >= now() - 600
    AND log_comment LIKE '04676_seal_%';

DROP TABLE t_adaptive_repl_left;
DROP TABLE t_adaptive_repl_right;
