SELECT 'Array product with constant column';

SELECT arrayProduct([1,2,3,4,5,6]) as a, toTypeName(a);
SELECT arrayProduct(array(1.0,2.0,3.0,4.0)) as a, toTypeName(a);
SELECT arrayProduct(array(1,3.5)) as a, toTypeName(a);
SELECT arrayProduct([toDecimal64(1,8), toDecimal64(2,8), toDecimal64(3,8)]) as a, toTypeName(a);

SELECT 'Array product with non constant column';

DROP TABLE IF EXISTS test_aggregation;
CREATE TABLE test_aggregation (x Array(Int)) ENGINE=TinyLog;
INSERT INTO test_aggregation VALUES ([1,2,3,4]), ([]), ([1,2,3]);
SELECT arrayProduct(x) FROM test_aggregation;
DROP TABLE test_aggregation;

CREATE TABLE test_aggregation (x Array(Decimal64(8))) ENGINE=TinyLog;
INSERT INTO test_aggregation VALUES ([1,2,3,4]), ([]), ([1,2,3]);
SELECT arrayProduct(x) FROM test_aggregation;
DROP TABLE test_aggregation;

SELECT 'Types of aggregation result array product';
SELECT toTypeName(arrayProduct([toInt8(0)])), toTypeName(arrayProduct([toInt16(0)])), toTypeName(arrayProduct([toInt32(0)])), toTypeName(arrayProduct([toInt64(0)]));
SELECT toTypeName(arrayProduct([toUInt8(0)])), toTypeName(arrayProduct([toUInt16(0)])), toTypeName(arrayProduct([toUInt32(0)])), toTypeName(arrayProduct([toUInt64(0)]));
SELECT toTypeName(arrayProduct([toInt128(0)])), toTypeName(arrayProduct([toInt256(0)])), toTypeName(arrayProduct([toUInt256(0)]));
SELECT toTypeName(arrayProduct([toFloat32(0)])), toTypeName(arrayProduct([toFloat64(0)]));
SELECT toTypeName(arrayProduct([toDecimal32(0, 8)])), toTypeName(arrayProduct([toDecimal64(0, 8)])), toTypeName(arrayProduct([toDecimal128(0, 8)]));
