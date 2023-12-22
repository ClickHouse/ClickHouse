CREATE TABLE part_log_bytes_uncompressed (
    key UInt8,
    value UInt8
)
Engine=MergeTree()
ORDER BY key;

INSERT INTO part_log_bytes_uncompressed VALUES (1, 1);
INSERT INTO part_log_bytes_uncompressed VALUES (2, 1);

OPTIMIZE TABLE part_log_bytes_uncompressed FINAL;

ALTER TABLE part_log_bytes_uncompressed UPDATE value = 3 WHERE 1 = 1 SETTINGS mutations_sync=2;

INSERT INTO part_log_bytes_uncompressed VALUES (3, 1);
ALTER TABLE part_log_bytes_uncompressed DROP PART 'all_4_4_0' SETTINGS mutations_sync=2;

SYSTEM FLUSH LOGS;

SELECT event_type, table, part_name, bytes_uncompressed FROM system.part_log
WHERE event_date >= yesterday() AND database = currentDatabase() AND table = 'part_log_bytes_uncompressed'
ORDER BY event_time_microseconds;

DROP TABLE part_log_bytes_uncompressed;
