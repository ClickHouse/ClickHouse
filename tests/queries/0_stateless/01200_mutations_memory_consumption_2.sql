-- Tags: no-debug, no-parallel

DROP TABLE IF EXISTS table_with_multi_pk;

CREATE TABLE table_with_multi_pk
(
  key1 UInt8,
  key2 UInt32,
  key3 DateTime64(6, 'UTC'),
  value String
)
ENGINE = MergeTree
ORDER BY (key1, key2, key3);

INSERT INTO table_with_multi_pk SELECT number % 32, number, toDateTime('2019-10-01 00:00:00'), toString(number % 10) FROM numbers(30000000);

OPTIMIZE TABLE table_with_multi_pk final;

ALTER TABLE table_with_multi_pk DELETE WHERE key1 % 77 = 0 SETTINGS mutations_sync = 1;

SYSTEM FLUSH LOGS;

-- Memory usage for all mutations must be almost constant and less than
-- read_bytes
SELECT
  arrayDistinct(groupArray(if (read_bytes >= peak_memory_usage, [1], [read_bytes, peak_memory_usage])))
  FROM
      system.part_log
 WHERE event_type = 'MutatePart' AND table = 'table_with_multi_pk' AND database = currentDatabase();

DROP TABLE IF EXISTS table_with_multi_pk;
