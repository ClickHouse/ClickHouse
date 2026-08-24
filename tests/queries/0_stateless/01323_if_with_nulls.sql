SELECT if(1 = 0, toNullable(toUInt8(0)), NULL) AS x, toTypeName(x);
SELECT if(1 = 1, toNullable(toUInt8(0)), NULL) AS x, toTypeName(x);
SELECT if(1 = 1, NULL, toNullable(toUInt8(0))) AS x, toTypeName(x);
SELECT if(1 = 0, NULL, toNullable(toUInt8(0))) AS x, toTypeName(x);

SELECT if(toUInt8(0), NULL, toNullable(toUInt8(0))) AS x, if(x = 0, 'ok', 'fail');
SELECT if(toUInt8(1), NULL, toNullable(toUInt8(0))) AS x, if(x = 0, 'fail', 'ok');
SELECT if(toUInt8(1), toNullable(toUInt8(0)), NULL) AS x, if(x = 0, 'ok', 'fail');
SELECT if(toUInt8(0), toNullable(toUInt8(0)), NULL) AS x, if(x = 0, 'fail', 'ok');

SELECT if(x = 0, 'ok', 'fail') FROM (SELECT toNullable(toUInt8(0)) AS x);
SELECT if(x = 0, 'fail', 'ok') FROM (SELECT CAST(NULL, 'Nullable(UInt8)') AS x);
SELECT if(x = 0, 'fail', 'ok') FROM (SELECT materialize(CAST(NULL, 'Nullable(UInt8)')) AS x);

SELECT if(x = 0, 'ok', 'fail') FROM (SELECT if(toUInt8(1), toNullable(toUInt8(0)), NULL) AS x);
SELECT if(x = 0, 'fail', 'ok') FROM (SELECT if(toUInt8(0), toNullable(toUInt8(0)), NULL) AS x);

SELECT if(x = 0, 'ok', 'fail') FROM (SELECT if(toUInt8(0), NULL, toNullable(toUInt8(0))) AS x);
SELECT if(x = 0, 'fail', 'ok') FROM (SELECT if(toUInt8(1), NULL, toNullable(toUInt8(0))) AS x);

SELECT toTypeName(x), x, isNull(x), if(x = 0, 'fail', 'ok'), if(x = 1, 'fail', 'ok'), if(x >= 0, 'fail', 'ok')
FROM (SELECT CAST(NULL, 'Nullable(UInt8)') AS x);

SELECT toTypeName(x), x, isNull(x), if(x = 0, 'fail', 'ok'), if(x = 1, 'fail', 'ok'), if(x >= 0, 'fail', 'ok')
FROM (SELECT materialize(CAST(NULL, 'Nullable(UInt8)')) AS x);

SELECT toTypeName(x), x, isNull(x), if(x = 0, 'fail', 'ok'), if(x = 1, 'fail', 'ok'), if(x >= 0, 'fail', 'ok')
FROM (SELECT if(1 = 0, toNullable(toUInt8(0)), NULL) AS x);

SELECT toTypeName(x), x, isNull(x), if(x = 0, 'fail', 'ok'), if(x = 1, 'fail', 'ok'), if(x >= 0, 'fail', 'ok')
FROM (SELECT materialize(if(1 = 0, toNullable(toUInt8(0)), NULL)) AS x);

SET join_use_nulls = 1;

SELECT b_num, isNull(b_num), toTypeName(b_num), b_num = 0, if(b_num = 0, 'fail', 'ok')
FROM (SELECT 1 k, toInt8(1) a_num) AS x
LEFT JOIN (SELECT 2 k, toInt8(1) b_num) AS y
USING (k);

-- test case from https://github.com/ClickHouse/ClickHouse/issues/7347
DROP TABLE IF EXISTS test_nullable_float_issue7347;
CREATE TABLE test_nullable_float_issue7347 (ne UInt64,test Nullable(Float64)) ENGINE = MergeTree() PRIMARY KEY (ne) ORDER BY (ne);
INSERT INTO test_nullable_float_issue7347 VALUES (1,NULL);

SELECT test, toTypeName(test), IF(test = 0, 1, 0) FROM test_nullable_float_issue7347;

WITH materialize(CAST(NULL, 'Nullable(Float64)')) AS test SELECT test, toTypeName(test), IF(test = 0, 1, 0);

DROP TABLE test_nullable_float_issue7347;

-- test case from https://github.com/ClickHouse/ClickHouse/issues/10846

SELECT if(isFinite(toUInt64OrZero(toNullable('123'))), 1, 0);

SELECT if(materialize(isFinite(toUInt64OrZero(toNullable('123')))), 1, 0);
