-- Tags: zookeeper, no-async-insert
-- no-async-insert: the test asserts insert deduplication of repeated INSERT VALUES, which does
-- not work when they are converted to asynchronous inserts (async_insert_deduplicate is off).

-- `presort_inserts_with_materialized_views` with ReplicatedMergeTree source and view target:
-- data integrity and the already-sorted fast path must work the same way as for plain MergeTree
-- (the replicated sink shares the sorting code but has its own deduplication machinery).

DROP TABLE IF EXISTS src_r;
DROP TABLE IF EXISTS tgt_r;
DROP TABLE IF EXISTS mv_r;

CREATE TABLE src_r (k UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04502_src', 'r1') ORDER BY k;
CREATE TABLE tgt_r (k UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04502_tgt', 'r1') ORDER BY k;
CREATE MATERIALIZED VIEW mv_r TO tgt_r AS SELECT k, v FROM src_r WHERE v % 2 = 0;

INSERT INTO src_r SELECT cityHash64(number) AS k, number AS v FROM numbers(100000)
SETTINGS presort_inserts_with_materialized_views = 1, log_comment = '04502_presort_replicated',
    max_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0,
    max_insert_threads = 1, max_threads = 1,
    use_async_executor_for_materialized_views = 0, parallel_view_processing = 0;

SELECT count(), sum(k = cityHash64(v)), sum(v) FROM src_r;
SELECT count(), sum(k = cityHash64(v)), sum(v) FROM tgt_r;

-- Both the source sink and the view target sink must have received sorted blocks.
SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['MergeTreeDataWriterBlocks'] > 0,
    ProfileEvents['MergeTreeDataWriterBlocks'] = ProfileEvents['MergeTreeDataWriterBlocksAlreadySorted']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND log_comment = '04502_presort_replicated';

-- Replicated deduplication with the presort enabled: the same block inserted twice must be
-- deduplicated in both tables, also when the setting is toggled between the retries.
INSERT INTO src_r SETTINGS presort_inserts_with_materialized_views = 1, insert_deduplicate = 1 VALUES (5, 0), (3, 1), (4, 2), (1, 3), (2, 4);
INSERT INTO src_r SETTINGS presort_inserts_with_materialized_views = 1, insert_deduplicate = 1 VALUES (5, 0), (3, 1), (4, 2), (1, 3), (2, 4);
INSERT INTO src_r SETTINGS presort_inserts_with_materialized_views = 0, insert_deduplicate = 1 VALUES (5, 0), (3, 1), (4, 2), (1, 3), (2, 4);

SELECT count(), sum(k), sum(v) FROM src_r WHERE k <= 5;
SELECT count(), sum(k), sum(v) FROM tgt_r WHERE k <= 5;

DROP TABLE mv_r;
DROP TABLE tgt_r;
DROP TABLE src_r;
