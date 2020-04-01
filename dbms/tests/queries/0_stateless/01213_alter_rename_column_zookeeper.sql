DROP TABLE IF EXISTS table_for_rename_replicated;


CREATE TABLE table_for_rename_replicated
(
  date Date,
  key UInt64,
  value1 String,
  value2 String,
  value3 String
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/table_for_rename_replicated', '1')
PARTITION BY date
ORDER BY key;


INSERT INTO table_for_rename_replicated SELECT toDate('2019-10-01') + number % 3, number, toString(number), toString(number), toString(number) from numbers(9);

SELECT value1 FROM table_for_rename_replicated WHERE key = 1;

SYSTEM STOP MERGES;

SHOW CREATE TABLE table_for_rename_replicated;

ALTER TABLE table_for_rename_replicated RENAME COLUMN value1 to renamed_value1 SETTINGS replication_alter_partitions_sync = 0;

SELECT sleep(2) FORMAT Null;

SHOW CREATE TABLE table_for_rename_replicated;

SELECT renamed_value1 FROM table_for_rename_replicated WHERE key = 1;

SELECT * FROM table_for_rename_replicated WHERE key = 1 FORMAT TSVWithNames;

SYSTEM START MERGES;

DROP TABLE IF EXISTS table_for_rename_replicated;
