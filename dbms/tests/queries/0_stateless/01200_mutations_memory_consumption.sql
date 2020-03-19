DROP TABLE IF EXISTS table_with_pk;

CREATE TABLE table_with_pk
(
  key UInt8,
  value String
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO table_with_pk SELECT number, toString(number % 10) FROM numbers(10000000);

ALTER TABLE table_with_pk DELETE WHERE key % 77 = 0 SETTINGS mutations_sync = 1;

SYSTEM FLUSH LOGS;

-- Memory usage for all mutations must be almost constant and less than
-- read_bytes.
SELECT
  DISTINCT read_bytes >= peak_memory_usage
FROM
    system.part_log2
WHERE event_type = 'MutatePart' AND table = 'table_with_pk' AND database = currentDatabase();

DROP TABLE IF EXISTS table_with_pk;
