-- Regression for the read-time consumer of a JOIN runtime filter (index analysis).
--
-- A runtime filter is built by N parallel streams and only becomes "finished" after the last
-- stream merges. The row-level probe (IRuntimeFilter::find) already fails open before that. The
-- read-time index-analysis path (getRecordedKeyValues -> exact IN-set granule pruning in
-- ReadFromMergeTree) must fail open too: reading the exact-value set before the finish flag is
-- published would both race the merging stream and expose only the keys merged so far, pruning
-- probe granules for not-yet-merged keys and silently dropping matching rows. getRecordedKeyRanges
-- already guards on the finish flag; getRecordedKeyValues must match it.
--
-- The dim side is small enough (4000 distinct keys < join_runtime_filter_exact_values_limit) to
-- keep the exact set (not overflow to a bloom filter) and selective enough that the exact IN-set
-- prunes most probe granules, so getRecordedKeyValues is really on the pruning path. It is kept in
-- several parts so the filter is built by several parallel streams (the multi-stream merge is what
-- creates the unfinished window). Correctness must hold with the feature on: a partial or racily
-- read set would prune granules for not-yet-merged keys and undercount.

DROP TABLE IF EXISTS rf_fact;
DROP TABLE IF EXISTS rf_dim;

CREATE TABLE rf_fact (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;
CREATE TABLE rf_dim (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;

-- Keep the build side in several parts -> several parallel build streams (more than one stream to
-- merge is what opens the unfinished window).
SYSTEM STOP MERGES rf_dim;

INSERT INTO rf_fact SELECT number, number FROM numbers(100000);
-- 8 parts, 500 keys each = 4000 distinct keys, clustered in [0, 8000) so exact-set pruning is selective.
INSERT INTO rf_dim SELECT number * 16 + 0  FROM numbers(500);
INSERT INTO rf_dim SELECT number * 16 + 2  FROM numbers(500);
INSERT INTO rf_dim SELECT number * 16 + 4  FROM numbers(500);
INSERT INTO rf_dim SELECT number * 16 + 6  FROM numbers(500);
INSERT INTO rf_dim SELECT number * 16 + 8  FROM numbers(500);
INSERT INTO rf_dim SELECT number * 16 + 10 FROM numbers(500);
INSERT INTO rf_dim SELECT number * 16 + 12 FROM numbers(500);
INSERT INTO rf_dim SELECT number * 16 + 14 FROM numbers(500);

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET use_skip_indexes_on_data_read = 1;
SET enable_parallel_replicas = 0;
SET join_algorithm = 'hash';
-- Keep rf_dim on the build side (its 8 parts feed the parallel build streams).
SET query_plan_join_swap_table = 0;
-- A fabricated probe-side row count from randomized join-order statistics can land at or below
-- the threshold, in which case no runtime filter is built at all.
SET query_plan_optimize_join_order_randomize = 0;
SET join_runtime_filter_min_probe_rows = 0;

-- Correctness: with index analysis on, every one of the 4000 matching rows must survive. A partial
-- or racily read exact set would prune probe granules for not-yet-merged keys and undercount.
-- max_threads spreads the build across parallel streams to exercise the merge window.
SELECT count() = 4000 FROM rf_fact AS f INNER JOIN rf_dim AS d ON f.id = d.id
SETTINGS enable_join_runtime_filters_index_analysis = 1, max_threads = 16, log_comment = '04614_probe';

SYSTEM FLUSH LOGS query_log, text_log;

-- Pruning must go through the exact IN-set, not the range fallback: the keys densely cover
-- [0, 7998], so a range predicate drops granules just as well and `RuntimeFilterGranulesDropped`
-- alone cannot tell the two apart.
SELECT count() > 0
FROM system.text_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
      -- Newest matching query only: clickhouse-test reuses the database across runs.
      AND query_id = (SELECT argMax(query_id, event_time_microseconds) FROM system.query_log
                      WHERE current_database = currentDatabase() AND log_comment = '04614_probe'
                      AND type = 'QueryFinish' AND event_date >= yesterday())
      AND message LIKE 'Index analysis engaged on join key %: pruning by exact IN-set of 4000 value(s)';

-- Pruning must actually drop granules, otherwise the correctness check above is vacuous.
SELECT argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time_microseconds) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04614_probe' AND type = 'QueryFinish';

DROP TABLE rf_fact;
DROP TABLE rf_dim;
