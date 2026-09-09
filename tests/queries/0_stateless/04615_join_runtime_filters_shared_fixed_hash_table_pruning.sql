-- Regression for the shared fixed-hash-table runtime filter publication path.
--
-- When the build side converts to a `FixedHashMap`, `HashJoin::publishSharedRuntimeFilters`
-- replaces the `Set`/bloom runtime filter with a `SharedFixedHashTableRuntimeFilter`, snapshotting
-- the recorded exact key values and key range once. `SharedFixedHashTableRuntimeFilter::merge` is a
-- no-op, so a snapshot taken while the build streams are still merging would stay empty for the
-- rest of the query, silently disabling index pruning instead of only during the unfinished window.
-- The publication is therefore skipped while the original filter is unfinished. Whichever filter
-- ends up published, results must be exact and granule pruning must still engage.

DROP TABLE IF EXISTS rf_shared_fact;
DROP TABLE IF EXISTS rf_shared_dim;

CREATE TABLE rf_shared_fact (id UInt32, v UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;
CREATE TABLE rf_shared_dim (id UInt32) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;

-- Keep the build side in several parts -> several parallel build streams, so the filter goes
-- through the multi-stream merge that opens the unfinished window.
SYSTEM STOP MERGES rf_shared_dim;

INSERT INTO rf_shared_fact SELECT number, number FROM numbers(100000);
-- 8 parts, 500 keys each = 4000 distinct keys in a dense, narrow range, so the build hash table is
-- converted to a `FixedHashMap` and the shared filter publication path is reached.
INSERT INTO rf_shared_dim SELECT number * 16 + 0  FROM numbers(500);
INSERT INTO rf_shared_dim SELECT number * 16 + 2  FROM numbers(500);
INSERT INTO rf_shared_dim SELECT number * 16 + 4  FROM numbers(500);
INSERT INTO rf_shared_dim SELECT number * 16 + 6  FROM numbers(500);
INSERT INTO rf_shared_dim SELECT number * 16 + 8  FROM numbers(500);
INSERT INTO rf_shared_dim SELECT number * 16 + 10 FROM numbers(500);
INSERT INTO rf_shared_dim SELECT number * 16 + 12 FROM numbers(500);
INSERT INTO rf_shared_dim SELECT number * 16 + 14 FROM numbers(500);

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET join_runtime_filter_from_fixed_hash_table = 1;
SET use_skip_indexes_on_data_read = 1;
SET enable_parallel_replicas = 0;
SET join_algorithm = 'hash';
-- The fixed-hash-table conversion (and therefore the shared filter publication) is skipped for
-- joins that may spill, so keep the build side in memory.
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
-- Keep rf_shared_dim on the build side (its 8 parts feed the parallel build streams).
SET query_plan_join_swap_table = 0;
-- A fabricated probe-side row count from randomized join-order statistics can land at or below
-- the threshold, in which case no runtime filter is built at all.
SET query_plan_optimize_join_order_randomize = 0;
SET join_runtime_filter_min_probe_rows = 0;

-- Correctness: every one of the 4000 matching rows must survive.
SELECT count() = 4000 FROM rf_shared_fact AS f INNER JOIN rf_shared_dim AS d ON f.id = d.id
SETTINGS max_threads = 16, log_comment = '04615_probe';

SYSTEM FLUSH LOGS query_log, text_log;

-- The publication itself must be observed: the original Set/bloom filter prunes on its own once it
-- finishes, so `RuntimeFilterGranulesDropped` moves whether or not `replace` was called.
SELECT count() > 0
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
      -- Newest matching query only: clickhouse-test reuses the database across runs, so an earlier
      -- run's publication must not satisfy this run's assertion.
      AND query_id = (SELECT argMax(query_id, event_time_microseconds) FROM system.query_log
                      WHERE current_database = currentDatabase() AND log_comment = '04615_probe'
                      AND type = 'QueryFinish' AND event_date >= yesterday())
      AND message LIKE 'Published shared fixed-hash-table runtime filter%';

-- Pruning must still engage: a filter published with an empty snapshot drops zero granules.
SELECT argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time_microseconds) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04615_probe' AND type = 'QueryFinish';

DROP TABLE rf_shared_fact;
DROP TABLE rf_shared_dim;
