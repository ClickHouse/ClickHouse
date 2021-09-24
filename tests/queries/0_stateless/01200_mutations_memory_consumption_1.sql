-- Tags: no-debug, no-parallel

DROP TABLE IF EXISTS table_with_single_pk;

CREATE TABLE table_with_single_pk
(
  key UInt8,
  value String
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO table_with_single_pk SELECT number, toString(number % 10) FROM numbers(30000000);

OPTIMIZE TABLE table_with_single_pk final;

ALTER TABLE table_with_single_pk DELETE WHERE key % 77 = 0 SETTINGS mutations_sync = 1;

SYSTEM FLUSH LOGS;

-- Memory usage for all mutations must be almost constant and less than
-- read_bytes
SELECT
  arrayDistinct(groupArray(if (read_bytes >= peak_memory_usage, [1], [read_bytes, peak_memory_usage])))
FROM
    system.part_log
WHERE event_type = 'MutatePart' AND table = 'table_with_single_pk' AND database = currentDatabase();

DROP TABLE IF EXISTS table_with_single_pk;
