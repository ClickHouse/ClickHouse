DROP TABLE IF EXISTS test;

CREATE TABLE test (key UInt64, val UInt64) engine = MergeTree Order by key PARTITION BY key >= 128;

SET max_block_size = 64, max_insert_block_size = 64, min_insert_block_size_rows = 64;

INSERT INTO test SELECT number AS key, sipHash64(number) AS val FROM numbers(512);

SYSTEM FLUSH LOGS;

SELECT
    if(count(DISTINCT query_id) == 1, 'Ok', 'Error: ' || toString(count(DISTINCT query_id))),
    if(count() == 512 / 64, 'Ok', 'Error: ' || toString(count())), -- 512 rows inserted, 64 rows per block
    if(SUM(ProfileEvents['MergeTreeDataWriterRows']) == 512, 'Ok', 'Error: ' || toString(SUM(ProfileEvents['MergeTreeDataWriterRows']))),
    if(SUM(ProfileEvents['MergeTreeDataWriterUncompressedBytes']) >= 1024, 'Ok', 'Error: ' || toString(SUM(ProfileEvents['MergeTreeDataWriterUncompressedBytes']))),
    if(SUM(ProfileEvents['MergeTreeDataWriterCompressedBytes']) >= 1024, 'Ok', 'Error: ' || toString(SUM(ProfileEvents['MergeTreeDataWriterCompressedBytes']))),
    if(SUM(ProfileEvents['MergeTreeDataWriterBlocks']) >= 8, 'Ok', 'Error: ' || toString(SUM(ProfileEvents['MergeTreeDataWriterBlocks'])))
FROM system.part_log
WHERE event_time > now() - INTERVAL 10 MINUTE
    AND database == currentDatabase() AND table == 'test'
    AND event_type == 'NewPart'
;

OPTIMIZE TABLE test FINAL;

SYSTEM FLUSH LOGS;
SELECT
    if(count() > 2, 'Ok', 'Error: ' || toString(count())),
    if(SUM(ProfileEvents['MergedRows']) >= 512, 'Ok', 'Error: ' || toString(SUM(ProfileEvents['MergedRows'])))
FROM system.part_log
WHERE event_time > now() - INTERVAL 10 MINUTE
    AND database == currentDatabase() AND table == 'test'
    AND event_type == 'MergeParts'
;

ALTER TABLE test UPDATE val = 0 WHERE key % 2 == 0 SETTINGS mutations_sync = 2;

SYSTEM FLUSH LOGS;

SELECT
    if(count() == 2, 'Ok', 'Error: ' || toString(count())),
    if(SUM(ProfileEvents['MutatedRows']) == 512, 'Ok', 'Error: ' || toString(SUM(ProfileEvents['MutatedRows']))),
    if(SUM(ProfileEvents['FileOpen']) > 1, 'Ok', 'Error: ' || toString(SUM(ProfileEvents['FileOpen'])))
FROM system.part_log
WHERE event_time > now() - INTERVAL 10 MINUTE
    AND database == currentDatabase() AND table == 'test'
    AND event_type == 'MutatePart'
;

DROP TABLE test;
