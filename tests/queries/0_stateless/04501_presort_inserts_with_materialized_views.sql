-- Tags: no-async-insert
-- no-async-insert: the test asserts insert deduplication of repeated INSERT VALUES, which does
-- not work when they are converted to asynchronous inserts (async_insert_deduplicate is off).

-- Test for the `presort_inserts_with_materialized_views` setting: data integrity, the
-- already-sorted fast path in the sinks, and the gates that must disable the presort.

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS tgt;
DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS src_replacing;
DROP TABLE IF EXISTS tgt_replacing;
DROP TABLE IF EXISTS mv_replacing;
DROP TABLE IF EXISTS rmv_mixed_on;
DROP TABLE IF EXISTS mv_mixed_on;
DROP TABLE IF EXISTS rtgt_mixed_on;
DROP TABLE IF EXISTS tgt_mixed_on;
DROP TABLE IF EXISTS src_mixed_on;
DROP TABLE IF EXISTS rmv_mixed_off;
DROP TABLE IF EXISTS rtgt_mixed_off;
DROP TABLE IF EXISTS src_mixed_off;
DROP TABLE IF EXISTS mv_dedup;
DROP TABLE IF EXISTS tgt_dedup;
DROP TABLE IF EXISTS src_dedup;

CREATE TABLE src (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE tgt (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW mv TO tgt AS SELECT k, v FROM src WHERE v % 2 = 0;

-- Insert data that is not sorted by the sorting key. The block sizes are pinned so that the
-- whole insert travels as a single block: otherwise the squashing on the view side may
-- concatenate several individually sorted blocks into an unsorted one, which is valid but
-- would make the profile-event checks below unstable under settings randomization.
INSERT INTO src SELECT cityHash64(number) AS k, number AS v FROM numbers(100000)
SETTINGS presort_inserts_with_materialized_views = 1, log_comment = '04501_presort_active',
    max_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0,
    max_insert_threads = 1, max_threads = 1,
    use_async_executor_for_materialized_views = 0, parallel_view_processing = 0;

SELECT count(), sum(k = cityHash64(v)), sum(v) FROM src;
SELECT count(), sum(k = cityHash64(v)), sum(v) FROM tgt;
SELECT count() FROM tgt WHERE v % 2 != 0;

-- Both the destination sink and the view target sink must have received sorted blocks.
SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['MergeTreeDataWriterBlocks'] > 0,
    ProfileEvents['MergeTreeDataWriterBlocks'] = ProfileEvents['MergeTreeDataWriterBlocksAlreadySorted']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND log_comment = '04501_presort_active';

-- A view targeting ReplacingMergeTree without a version column resolves rows with equal keys
-- positionally. When it is the only view, no branch benefits from the presort, so the presort
-- must be skipped and both sinks must sort the blocks themselves.
CREATE TABLE src_replacing (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE tgt_replacing (m UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY m;
CREATE MATERIALIZED VIEW mv_replacing TO tgt_replacing AS SELECT k % 10 AS m, v FROM src_replacing;

INSERT INTO src_replacing SELECT cityHash64(number) AS k, number AS v FROM numbers(100000)
SETTINGS presort_inserts_with_materialized_views = 1, log_comment = '04501_presort_gated',
    max_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0,
    max_insert_threads = 1, max_threads = 1,
    use_async_executor_for_materialized_views = 0, parallel_view_processing = 0;

SELECT count(), sum(k = cityHash64(v)) FROM src_replacing;

SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['MergeTreeDataWriterBlocks'] > 0,
    ProfileEvents['MergeTreeDataWriterBlocksAlreadySorted']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND log_comment = '04501_presort_gated';

-- Mixed case: an order-insensitive view and a versionless Replacing view on the same source.
-- The presort must run (the plain target benefits and skips its sort), while the Replacing view
-- must observe the rows restored to the original insertion order: its FINAL result must be
-- identical to the result of the same insert made with the setting disabled.
CREATE TABLE src_mixed_on (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE tgt_mixed_on (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE rtgt_mixed_on (m UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY m;
CREATE MATERIALIZED VIEW mv_mixed_on TO tgt_mixed_on AS SELECT k, v FROM src_mixed_on;
CREATE MATERIALIZED VIEW rmv_mixed_on TO rtgt_mixed_on AS SELECT k % 10 AS m, v FROM src_mixed_on;

CREATE TABLE src_mixed_off (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE rtgt_mixed_off (m UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY m;
CREATE MATERIALIZED VIEW rmv_mixed_off TO rtgt_mixed_off AS SELECT k % 10 AS m, v FROM src_mixed_off;

SYSTEM STOP MERGES rtgt_mixed_on;
SYSTEM STOP MERGES rtgt_mixed_off;

INSERT INTO src_mixed_on SELECT cityHash64(number) AS k, number AS v FROM numbers(100000)
SETTINGS presort_inserts_with_materialized_views = 1, log_comment = '04501_presort_mixed',
    max_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0,
    max_insert_threads = 1, max_threads = 1,
    use_async_executor_for_materialized_views = 0, parallel_view_processing = 0;

INSERT INTO src_mixed_off SELECT cityHash64(number) AS k, number AS v FROM numbers(100000)
SETTINGS presort_inserts_with_materialized_views = 0,
    max_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0,
    max_insert_threads = 1, max_threads = 1,
    use_async_executor_for_materialized_views = 0, parallel_view_processing = 0;

-- For a versionless ReplacingMergeTree the survivor among rows with equal keys is determined by
-- their order within the part, so equal FINAL results prove the view observed the original order.
SELECT (SELECT sum(cityHash64(m, v)) FROM rtgt_mixed_on FINAL) = (SELECT sum(cityHash64(m, v)) FROM rtgt_mixed_off FINAL);

-- Of the three sinks (src_mixed_on, tgt_mixed_on, rtgt_mixed_on), the first two must have
-- received sorted blocks; the Replacing one receives the restored (unsorted) order and sorts.
SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['MergeTreeDataWriterBlocks'],
    ProfileEvents['MergeTreeDataWriterBlocksAlreadySorted']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND log_comment = '04501_presort_mixed';

-- Insert deduplication with the presort enabled: the same block inserted twice must be
-- deduplicated in both the source and the view target, and the deduplication tokens must not
-- depend on the setting (they are derived from the block as it was before the presort), so a
-- retry with the setting toggled must be deduplicated as well. INSERT VALUES, not INSERT SELECT:
-- deduplication is disabled for INSERT SELECT.
CREATE TABLE src_dedup (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS non_replicated_deduplication_window = 100;
CREATE TABLE tgt_dedup (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS non_replicated_deduplication_window = 100;
CREATE MATERIALIZED VIEW mv_dedup TO tgt_dedup AS SELECT k, v FROM src_dedup WHERE v % 2 = 0;

INSERT INTO src_dedup SETTINGS presort_inserts_with_materialized_views = 1, insert_deduplicate = 1 VALUES (5, 0), (3, 1), (4, 2), (1, 3), (2, 4);
INSERT INTO src_dedup SETTINGS presort_inserts_with_materialized_views = 1, insert_deduplicate = 1 VALUES (5, 0), (3, 1), (4, 2), (1, 3), (2, 4);
INSERT INTO src_dedup SETTINGS presort_inserts_with_materialized_views = 0, insert_deduplicate = 1 VALUES (5, 0), (3, 1), (4, 2), (1, 3), (2, 4);

SELECT count(), sum(k), sum(v) FROM src_dedup;
SELECT count(), sum(k), sum(v) FROM tgt_dedup;

DROP TABLE mv;
DROP TABLE tgt;
DROP TABLE src;
DROP TABLE mv_dedup;
DROP TABLE tgt_dedup;
DROP TABLE src_dedup;
DROP TABLE mv_replacing;
DROP TABLE tgt_replacing;
DROP TABLE src_replacing;
DROP TABLE rmv_mixed_on;
DROP TABLE mv_mixed_on;
DROP TABLE rtgt_mixed_on;
DROP TABLE tgt_mixed_on;
DROP TABLE src_mixed_on;
DROP TABLE rmv_mixed_off;
DROP TABLE rtgt_mixed_off;
DROP TABLE src_mixed_off;
