DROP TABLE IF EXISTS test_table;
DROP TABLE IF EXISTS numbers;
DROP TABLE IF EXISTS test_mv;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS dist;

CREATE TABLE test_table (key UInt32, value Decimal(16, 6)) ENGINE = SummingMergeTree() ORDER BY key;
CREATE TABLE numbers (number UInt64) ENGINE=Memory;

CREATE MATERIALIZED VIEW test_mv TO test_table (number UInt64, value Decimal(38, 6))
AS SELECT number, sum(number) AS value FROM (SELECT *, toDecimal64(number, 6) AS val FROM numbers) GROUP BY number;

INSERT INTO numbers SELECT * FROM numbers(100000);

SELECT sum(value) FROM test_mv;
SELECT sum(value) FROM (SELECT number, sum(number) AS value FROM (SELECT *, toDecimal64(number, 6) AS val FROM numbers) GROUP BY number);

CREATE TABLE src (n UInt64, s FixedString(16)) ENGINE=Memory;
CREATE TABLE dst (n UInt8, s String) ENGINE = Memory;
CREATE MATERIALIZED VIEW mv TO dst (n String) AS SELECT * FROM src;
SET allow_experimental_bigint_types=1;
CREATE TABLE dist (n Int128) ENGINE=Distributed(test_cluster_two_shards, currentDatabase(), mv);

INSERT INTO src SELECT number, toString(number) FROM numbers(1000);
INSERT INTO mv SELECT toString(number + 1000) FROM numbers(1000); -- { serverError TYPE_MISMATCH }
INSERT INTO mv SELECT arrayJoin(['42', 'test']); -- { serverError TYPE_MISMATCH }

SELECT count(), sum(n), sum(toInt64(s)), max(n), min(n) FROM src;
SELECT count(), sum(n), sum(toInt64(s)), max(n), min(n) FROM dst;
SELECT count(), sum(toInt64(n)), max(n), min(n) FROM mv;
SELECT count(), sum(toInt64(n)), max(n), min(n) FROM dist; -- { serverError CANNOT_CONVERT_TYPE }
SELECT count(), sum(toInt64(n)), max(toUInt32(n)), min(toInt128(n)) FROM dist;

DROP TABLE test_table;
DROP TABLE numbers;
DROP TABLE test_mv;
DROP TABLE src;
DROP TABLE dst;
DROP TABLE mv;
DROP TABLE dist;
