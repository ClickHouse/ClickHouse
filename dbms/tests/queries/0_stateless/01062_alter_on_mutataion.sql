DROP TABLE IF EXISTS test_alter_on_mutation;

CREATE TABLE test_alter_on_mutation
(
  date Date,
  key UInt64,
  value String
)
ENGINE ReplicatedMergeTree('/clickhouse/tables/test_alter_on_mutation', '1')
ORDER BY key PARTITION BY date;

INSERT INTO test_alter_on_mutation select toDate('2020-01-05'), number, toString(number) from system.numbers limit 100;
INSERT INTO test_alter_on_mutation select toDate('2020-01-06'), number, toString(number) from system.numbers limit 100;
INSERT INTO test_alter_on_mutation select toDate('2020-01-07'), number, toString(number) from system.numbers limit 100;

SELECT sum(cast(value as UInt64)) from test_alter_on_mutation;

ALTER TABLE test_alter_on_mutation MODIFY COLUMN value UInt64;

SELECT sum(value) from test_alter_on_mutation;

INSERT INTO test_alter_on_mutation select toDate('2020-01-05'), number, toString(number) from system.numbers limit 100, 100;
INSERT INTO test_alter_on_mutation select toDate('2020-01-06'), number, toString(number) from system.numbers limit 100, 100;
INSERT INTO test_alter_on_mutation select toDate('2020-01-07'), number, toString(number) from system.numbers limit 100, 100;

OPTIMIZE TABLE test_alter_on_mutation FINAL;

SELECT sum(value) from test_alter_on_mutation;

ALTER TABLE test_alter_on_mutation MODIFY COLUMN value String;

SELECT sum(cast(value as UInt64)) from test_alter_on_mutation;

OPTIMIZE TABLE test_alter_on_mutation FINAL;

SELECT sum(cast(value as UInt64)) from test_alter_on_mutation;

ALTER TABLE test_alter_on_mutation ADD COLUMN value1 Float64;

SELECT sum(value1) from test_alter_on_mutation;

ALTER TABLE test_alter_on_mutation DROP COLUMN value;

SELECT sum(value) from test_alter_on_mutation; -- {serverError 47}

ALTER TABLE test_alter_on_mutation ADD COLUMN value String DEFAULT '10';

SELECT sum(cast(value as UInt64)) from test_alter_on_mutation;

--OPTIMIZE table test_alter_on_mutation FINAL;

ALTER TABLE test_alter_on_mutation MODIFY COLUMN value UInt64;

SELECT sum(value) from test_alter_on_mutation;

DROP TABLE IF EXISTS test_alter_on_mutation;
