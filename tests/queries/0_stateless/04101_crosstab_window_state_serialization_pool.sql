-- cramersV
DROP TABLE IF EXISTS test_cv;
CREATE TABLE test_cv (s AggregateFunction(cramersV, UInt8, UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_cv SELECT cramersVState(toUInt8(number % 10), toUInt8(number % 6)) OVER () FROM numbers(100) LIMIT 1;
SELECT length(hex(cramersVState(toUInt8(number % 17), toUInt8(number % 23)) OVER ())) > 0 FROM numbers(1000) LIMIT 1;
DROP TABLE test_cv;

-- contingency
DROP TABLE IF EXISTS test_ct;
CREATE TABLE test_ct (s AggregateFunction(contingency, UInt8, UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_ct SELECT contingencyState(toUInt8(number % 10), toUInt8(number % 6)) OVER () FROM numbers(100) LIMIT 1;
SELECT length(hex(contingencyState(toUInt8(number % 17), toUInt8(number % 23)) OVER ())) > 0 FROM numbers(1000) LIMIT 1;
DROP TABLE test_ct;

-- cramersVBiasCorrected
DROP TABLE IF EXISTS test_cvbc;
CREATE TABLE test_cvbc (s AggregateFunction(cramersVBiasCorrected, UInt8, UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_cvbc SELECT cramersVBiasCorrectedState(toUInt8(number % 10), toUInt8(number % 6)) OVER () FROM numbers(100) LIMIT 1;
SELECT length(hex(cramersVBiasCorrectedState(toUInt8(number % 17), toUInt8(number % 23)) OVER ())) > 0 FROM numbers(1000) LIMIT 1;
DROP TABLE test_cvbc;

-- theilsU
DROP TABLE IF EXISTS test_tu;
CREATE TABLE test_tu (s AggregateFunction(theilsU, UInt8, UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_tu SELECT theilsUState(toUInt8(number % 10), toUInt8(number % 6)) OVER () FROM numbers(100) LIMIT 1;
SELECT length(hex(theilsUState(toUInt8(number % 17), toUInt8(number % 23)) OVER ())) > 0 FROM numbers(1000) LIMIT 1;
DROP TABLE test_tu;
