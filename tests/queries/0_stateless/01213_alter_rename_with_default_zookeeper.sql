DROP TABLE IF EXISTS table_rename_with_default;

CREATE TABLE table_rename_with_default
(
  date Date,
  key UInt64,
  value1 String,
  value2 String DEFAULT concat('Hello ', value1),
  value3 String ALIAS concat('Word ', value1)
)
ENGINE = MergeTree()
PARTITION BY date
ORDER BY key;

INSERT INTO table_rename_with_default (date, key, value1) SELECT toDate('2019-10-01') + number % 3, number, toString(number)  from numbers(9);

SELECT * FROM table_rename_with_default WHERE key = 1 FORMAT TSVWithNames;

SHOW CREATE TABLE table_rename_with_default;

ALTER TABLE table_rename_with_default RENAME COLUMN value1 TO renamed_value1;

SELECT * FROM table_rename_with_default WHERE key = 1 FORMAT TSVWithNames;

SHOW CREATE TABLE table_rename_with_default;

SELECT value2 FROM table_rename_with_default WHERE key = 1;
SELECT value3 FROM table_rename_with_default WHERE key = 1;

DROP TABLE IF EXISTS table_rename_with_default;

DROP TABLE IF EXISTS table_rename_with_ttl;

CREATE TABLE table_rename_with_ttl
(
  date1 Date,
  date2 Date,
  value1 String,
  value2 String TTL date1 + INTERVAL 10000 MONTH
)
ENGINE = ReplicatedMergeTree('/clickhouse/test/table_rename_with_ttl', '1')
ORDER BY tuple()
TTL date2 + INTERVAL 10000 MONTH;

INSERT INTO table_rename_with_ttl SELECT toDate('2019-10-01') + number % 3, toDate('2018-10-01') + number % 3, toString(number), toString(number) from numbers(9);

SELECT * FROM table_rename_with_ttl WHERE value1 = '1' FORMAT TSVWithNames;

SHOW CREATE TABLE table_rename_with_ttl;

ALTER TABLE table_rename_with_ttl RENAME COLUMN date1 TO renamed_date1;

SELECT * FROM table_rename_with_ttl WHERE value1 = '1' FORMAT TSVWithNames;

SHOW CREATE TABLE table_rename_with_ttl;

ALTER TABLE table_rename_with_ttl RENAME COLUMN date2 TO renamed_date2;

SELECT * FROM table_rename_with_ttl WHERE value1 = '1' FORMAT TSVWithNames;

SHOW CREATE TABLE table_rename_with_ttl;

DROP TABLE IF EXISTS table_rename_with_ttl;
