DROP TABLE IF EXISTS test_alter_on_mutation;

CREATE TABLE test_alter_on_mutation
(
  date Date,
  key UInt64,
  value String
)
ENGINE ReplicatedMergeTree('/clickhouse/tables/test_01062/alter_on_mutation', '1')
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

-- TODO(alesap)
OPTIMIZE table test_alter_on_mutation FINAL;

ALTER TABLE test_alter_on_mutation MODIFY COLUMN value UInt64 DEFAULT 10;

SELECT sum(value) from test_alter_on_mutation;

DROP TABLE IF EXISTS test_alter_on_mutation;

DROP TABLE IF EXISTS nested_alter;

CREATE TABLE nested_alter (`d` Date, `k` UInt64, `i32` Int32, `dt` DateTime, `n.ui8` Array(UInt8), `n.s` Array(String), `n.d` Array(Date), `s` String DEFAULT '0') ENGINE = ReplicatedMergeTree('/clickhouse/tables/test_01062/nested_alter', 'r2', d, k, 8192);

INSERT INTO nested_alter VALUES ('2015-01-01', 6,38,'2014-07-15 13:26:50',[10,20,30],['asd','qwe','qwe'],['2000-01-01','2000-01-01','2000-01-03'],'100500');

SELECT * FROM nested_alter;

ALTER TABLE nested_alter DROP COLUMN `n.d`;

SELECT * FROM nested_alter;

DROP TABLE nested_alter;
