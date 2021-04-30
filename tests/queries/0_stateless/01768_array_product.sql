SELECT 'Array product ', (arrayProduct(array(1,2,3,4,5,6)));
select arrayProduct(array(1.0,2.0,3.0,4.8)) as k , toTypeName(k);
select arrayProduct(array(1,3.5)) as k , toTypeName(k);
SELECT arrayProduct([toDecimal32(2, 8), toDecimal32(10, 8)]) as a , toTypeName(a);

DROP TABLE IF EXISTS test_aggregation;
CREATE TABLE test_aggregation (x Array(Int)) ENGINE=TinyLog;
INSERT INTO test_aggregation VALUES ([1,2,3,4,5,6]), ([]), ([1,2,3]);
SELECT arrayProduct(x) FROM test_aggregation;
DROP TABLE test_aggregation;

CREATE TABLE test_aggregation (x Array(Decimal64(8))) ENGINE=TinyLog;
INSERT INTO test_aggregation VALUES ([1,2,3,4,5,6]), ([]), ([1,2,3]);
SELECT arrayProduct(x) FROM test_aggregation;
DROP TABLE test_aggregation;

SELECT 'Types of aggregation result array product';
SELECT toTypeName(arrayProduct([toInt8(0)])), toTypeName(arrayProduct([toInt16(0)])), toTypeName(arrayProduct([toInt32(0)])), toTypeName(arrayProduct([toInt64(0)]));
SELECT toTypeName(arrayProduct([toUInt8(0)])), toTypeName(arrayProduct([toUInt16(0)])), toTypeName(arrayProduct([toUInt32(0)])), toTypeName(arrayProduct([toUInt64(0)]));
SELECT toTypeName(arrayProduct([toInt128(0)])), toTypeName(arrayProduct([toInt256(0)])), toTypeName(arrayProduct([toUInt256(0)]));
SELECT toTypeName(arrayProduct([toFloat32(0)])), toTypeName(arrayProduct([toFloat64(0)]));
SELECT toTypeName(arrayProduct([toDecimal32(0, 8)])), toTypeName(arrayProduct([toDecimal64(0, 8)])), toTypeName(arrayProduct([toDecimal128(0, 8)]));
