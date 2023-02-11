-- Tags: no-parallel

DROP TABLE IF EXISTS data_01269;
CREATE TABLE data_01269
(
    key     Int32,
    value   Nullable(Int32),
    alias   UInt8 ALIAS value>0
)
ENGINE = MergeTree()
ORDER BY key;
INSERT INTO data_01269 VALUES (1, 0);

-- ubsan
-- https://s3.amazonaws.com/clickhouse-test-reports/45461/29f8958590c990f52b14d0fa786d5374fed842e3/stateless_tests__ubsan__[2/2].html

-- after PR#10441
SELECT toTypeName(alias) FROM data_01269;
SELECT any(alias) FROM data_01269;

-- even without PR#10441
ALTER TABLE data_01269 DROP COLUMN alias;
ALTER TABLE data_01269 ADD COLUMN alias UInt8 ALIAS value>0;
SELECT toTypeName(alias) FROM data_01269;
SELECT any(alias) FROM data_01269;


DROP TABLE data_01269;
