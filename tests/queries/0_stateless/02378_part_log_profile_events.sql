DROP TABLE IF EXISTS test;

CREATE TABLE test (key UInt64, val UInt64) engine = MergeTree Order by key PARTITION BY key >= 128;

SET max_block_size = 64, max_insert_block_size = 64, min_insert_block_size_rows = 64;

INSERT INTO test SELECT number AS key, sipHash64(number) AS val FROM numbers(512);

SYSTEM FLUSH LOGS;
SELECT
    count(DISTINCT query_id) == 1
    AND count() >= 512 / 64 -- 512 rows inserted, 64 rows per block
    AND SUM(ProfileEvents['MergeTreeDataWriterRows']) == 512
    AND SUM(ProfileEvents['MergeTreeDataWriterUncompressedBytes']) > 1024
    AND SUM(ProfileEvents['MergeTreeDataWriterCompressedBytes']) > 1024
    AND SUM(ProfileEvents['MergeTreeDataWriterBlocks']) >= 8
FROM system.part_log
WHERE event_time > now() - INTERVAL 10 MINUTE
    AND database == currentDatabase() AND table == 'test'
    AND event_type == 'NewPart'
;

OPTIMIZE TABLE test FINAL;

SYSTEM FLUSH LOGS;
SELECT
    count() >= 2 AND SUM(ProfileEvents['MergedRows']) >= 512
FROM system.part_log
WHERE event_time > now() - INTERVAL 10 MINUTE
    AND database == currentDatabase() AND table == 'test'
    AND event_type == 'MergeParts'
;

ALTER TABLE test UPDATE val = 0 WHERE key % 2 == 0 SETTINGS mutations_sync = 2;

SYSTEM FLUSH LOGS;
SELECT
    count() == 2
    AND SUM(ProfileEvents['SelectedRows']) == 512
    AND SUM(ProfileEvents['FileOpen']) > 2
FROM system.part_log
WHERE event_time > now() - INTERVAL 10 MINUTE
    AND database == currentDatabase() AND table == 'test'
    AND event_type == 'MutatePart'
;

DROP TABLE test;
