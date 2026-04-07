SELECT 'Array min ', (arrayMin(array(1,2,3,4,5,6)));
SELECT 'Array max ', (arrayMax(array(1,2,3,4,5,6)));
SELECT 'Array sum ', (arraySum(array(1,2,3,4,5,6)));
SELECT 'Array avg ', (arrayAvg(array(1,2,3,4,5,6)));

SELECT 'Array min :';
SELECT arrayMin([[3], [1], [2]]);

SELECT 'Array max :';
SELECT arrayMax([[3], [1], [2]]);

DROP TABLE IF EXISTS test_aggregation;
CREATE TABLE test_aggregation (x Array(Int)) ENGINE=TinyLog;

INSERT INTO test_aggregation VALUES ([1,2,3,4,5,6]), ([]), ([1,2,3]);

SELECT 'Table array int min';
SELECT arrayMin(x) FROM test_aggregation;
SELECT 'Table array int max';
SELECT arrayMax(x) FROM test_aggregation;
SELECT 'Table array int sum';
SELECT arraySum(x) FROM test_aggregation;
SELECT 'Table array int avg';
SELECT arrayAvg(x) FROM test_aggregation;

DROP TABLE test_aggregation;

CREATE TABLE test_aggregation (x Array(Decimal64(8))) ENGINE=TinyLog;

INSERT INTO test_aggregation VALUES ([1,2,3,4,5,6]), ([]), ([1,2,3]);

SELECT 'Table array decimal min';
SELECT arrayMin(x) FROM test_aggregation;
SELECT 'Table array decimal max';
SELECT arrayMax(x) FROM test_aggregation;
SELECT 'Table array decimal sum';
SELECT arraySum(x) FROM test_aggregation;
SELECT 'Table array decimal avg';
SELECT arrayAvg(x) FROM test_aggregation;

DROP TABLE test_aggregation;

WITH ['2023-04-05 00:25:23', '2023-04-05 00:25:24']::Array(DateTime) AS dt SELECT arrayMax(dt), arrayMin(dt), arrayDifference(dt);
WITH ['2023-04-05 00:25:23.123', '2023-04-05 00:25:24.124']::Array(DateTime64(3)) AS dt SELECT arrayMax(dt), arrayMin(dt), arrayDifference(dt);
WITH ['2023-04-05', '2023-04-06']::Array(Date) AS d SELECT arrayMax(d), arrayMin(d), arrayDifference(d);
WITH ['2023-04-05', '2023-04-06']::Array(Date32) AS d SELECT arrayMax(d), arrayMin(d), arrayDifference(d);

SELECT 'Types of aggregation result array min';
SELECT toTypeName(arrayMin([toInt8(0)])), toTypeName(arrayMin([toInt16(0)])), toTypeName(arrayMin([toInt32(0)])), toTypeName(arrayMin([toInt64(0)]));
SELECT toTypeName(arrayMin([toUInt8(0)])), toTypeName(arrayMin([toUInt16(0)])), toTypeName(arrayMin([toUInt32(0)])), toTypeName(arrayMin([toUInt64(0)]));
SELECT toTypeName(arrayMin([toInt128(0)])), toTypeName(arrayMin([toInt256(0)])), toTypeName(arrayMin([toUInt256(0)]));
SELECT toTypeName(arrayMin([toFloat32(0)])), toTypeName(arrayMin([toFloat64(0)]));
SELECT toTypeName(arrayMin([toDecimal32(0, 8)])), toTypeName(arrayMin([toDecimal64(0, 8)])), toTypeName(arrayMin([toDecimal128(0, 8)]));
SELECT 'Types of aggregation result array max';
SELECT toTypeName(arrayMax([toInt8(0)])), toTypeName(arrayMax([toInt16(0)])), toTypeName(arrayMax([toInt32(0)])), toTypeName(arrayMax([toInt64(0)]));
SELECT toTypeName(arrayMax([toUInt8(0)])), toTypeName(arrayMax([toUInt16(0)])), toTypeName(arrayMax([toUInt32(0)])), toTypeName(arrayMax([toUInt64(0)]));
SELECT toTypeName(arrayMax([toInt128(0)])), toTypeName(arrayMax([toInt256(0)])), toTypeName(arrayMax([toUInt256(0)]));
SELECT toTypeName(arrayMax([toFloat32(0)])), toTypeName(arrayMax([toFloat64(0)]));
SELECT toTypeName(arrayMax([toDecimal32(0, 8)])), toTypeName(arrayMax([toDecimal64(0, 8)])), toTypeName(arrayMax([toDecimal128(0, 8)]));
SELECT 'Types of aggregation result array summ';
SELECT toTypeName(arraySum([toInt8(0)])), toTypeName(arraySum([toInt16(0)])), toTypeName(arraySum([toInt32(0)])), toTypeName(arraySum([toInt64(0)]));
SELECT toTypeName(arraySum([toUInt8(0)])), toTypeName(arraySum([toUInt16(0)])), toTypeName(arraySum([toUInt32(0)])), toTypeName(arraySum([toUInt64(0)]));
SELECT toTypeName(arraySum([toInt128(0)])), toTypeName(arraySum([toInt256(0)])), toTypeName(arraySum([toUInt256(0)]));
SELECT toTypeName(arraySum([toFloat32(0)])), toTypeName(arraySum([toFloat64(0)]));
SELECT toTypeName(arraySum([toDecimal32(0, 8)])), toTypeName(arraySum([toDecimal64(0, 8)])), toTypeName(arraySum([toDecimal128(0, 8)]));
SELECT 'Types of aggregation result array avg';
SELECT toTypeName(arrayAvg([toInt8(0)])), toTypeName(arrayAvg([toInt16(0)])), toTypeName(arrayAvg([toInt32(0)])), toTypeName(arrayAvg([toInt64(0)]));
SELECT toTypeName(arrayAvg([toUInt8(0)])), toTypeName(arrayAvg([toUInt16(0)])), toTypeName(arrayAvg([toUInt32(0)])), toTypeName(arrayAvg([toUInt64(0)]));
SELECT toTypeName(arrayAvg([toInt128(0)])), toTypeName(arrayAvg([toInt256(0)])), toTypeName(arrayAvg([toUInt256(0)]));
SELECT toTypeName(arrayAvg([toFloat32(0)])), toTypeName(arrayAvg([toFloat64(0)]));
SELECT toTypeName(arrayAvg([toDecimal32(0, 8)])), toTypeName(arrayAvg([toDecimal64(0, 8)])), toTypeName(arrayAvg([toDecimal128(0, 8)]));
