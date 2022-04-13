-- Tags: long, zookeeper

DROP TABLE IF EXISTS alter_default;

CREATE TABLE alter_default
(
  date Date,
  key UInt64
)
ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_01079/alter_default', '1')
ORDER BY key;

INSERT INTO alter_default select toDate('2020-01-05'), number from system.numbers limit 100;

-- Cannot add column without type
ALTER TABLE alter_default ADD COLUMN value DEFAULT '10'; --{serverError 36}

ALTER TABLE alter_default ADD COLUMN value String DEFAULT '10';

SHOW CREATE TABLE alter_default;

SELECT sum(cast(value as UInt64)) FROM alter_default;

ALTER TABLE alter_default MODIFY COLUMN value UInt64;

SHOW CREATE TABLE alter_default;

ALTER TABLE alter_default MODIFY COLUMN value UInt64 DEFAULT 10;

SHOW CREATE TABLE alter_default;

SELECT sum(value) from alter_default;

ALTER TABLE alter_default MODIFY COLUMN value DEFAULT 100;

SHOW CREATE TABLE alter_default;

ALTER TABLE alter_default MODIFY COLUMN value UInt16 DEFAULT 100;

SHOW CREATE TABLE alter_default;

SELECT sum(value) from alter_default;

ALTER TABLE alter_default MODIFY COLUMN value UInt8 DEFAULT 10;

SHOW CREATE TABLE alter_default;

ALTER TABLE alter_default ADD COLUMN bad_column UInt8 DEFAULT 'q'; --{serverError 6}

ALTER TABLE alter_default ADD COLUMN better_column UInt8 DEFAULT '1';

SHOW CREATE TABLE alter_default;

ALTER TABLE alter_default ADD COLUMN other_date String DEFAULT '0';

ALTER TABLE alter_default MODIFY COLUMN other_date DateTime; --{serverError 41}

ALTER TABLE alter_default MODIFY COLUMN other_date DEFAULT 1;

SHOW CREATE TABLE alter_default;

DROP TABLE IF EXISTS alter_default;
