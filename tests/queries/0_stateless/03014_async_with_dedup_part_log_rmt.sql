CREATE TABLE 03014_async_with_dedup_part_log (x UInt64)
ENGINE=ReplicatedMergeTree('/clickhouse/table/{database}/03014_async_with_dedup_part_log', 'r1') ORDER BY tuple();

SET async_insert = 1;
SET wait_for_async_insert = 1;
SET async_insert_deduplicate = 1;

SELECT '-- Inserted part --';
INSERT INTO 03014_async_with_dedup_part_log VALUES (2);

SYSTEM FLUSH LOGS;
SELECT error, count() FROM system.part_log
WHERE table = '03014_async_with_dedup_part_log' AND database = currentDatabase() AND event_type = 'NewPart'
GROUP BY error
ORDER BY error;

SELECT '-- Deduplicated part --';
INSERT INTO 03014_async_with_dedup_part_log VALUES (2);

SYSTEM FLUSH LOGS;
SELECT error, count() FROM system.part_log
WHERE table = '03014_async_with_dedup_part_log' AND database = currentDatabase() AND event_type = 'NewPart'
GROUP BY error
ORDER BY error;
