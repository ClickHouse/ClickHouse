-- Tags: no-random-merge-tree-settings, no-random-settings
-- Because we compare part sizes, and they could be affected by index granularity and index compression settings.

CREATE TABLE part_log_bytes_uncompressed (
    key UInt8,
    value UInt8
)
Engine=MergeTree()
ORDER BY key;

INSERT INTO part_log_bytes_uncompressed SELECT 1, 1 FROM numbers(1000);
INSERT INTO part_log_bytes_uncompressed SELECT 2, 1 FROM numbers(1000);

OPTIMIZE TABLE part_log_bytes_uncompressed FINAL;

ALTER TABLE part_log_bytes_uncompressed UPDATE value = 3 WHERE 1 = 1 SETTINGS mutations_sync=2;

INSERT INTO part_log_bytes_uncompressed SELECT 3, 1 FROM numbers(1000);
ALTER TABLE part_log_bytes_uncompressed DROP PART 'all_4_4_0' SETTINGS mutations_sync=2;

SYSTEM FLUSH LOGS;

SELECT event_type, table, part_name, bytes_uncompressed > 0, (bytes_uncompressed > 0 ? (size_in_bytes < bytes_uncompressed ? '1' : toString((size_in_bytes, bytes_uncompressed))) : '0')
FROM system.part_log
WHERE event_date >= yesterday() AND database = currentDatabase() AND table = 'part_log_bytes_uncompressed'
    AND (event_type != 'RemovePart' OR part_name = 'all_4_4_0') -- ignore removal of other parts
ORDER BY part_name, event_type;

DROP TABLE part_log_bytes_uncompressed;
