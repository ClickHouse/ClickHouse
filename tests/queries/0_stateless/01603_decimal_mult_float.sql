SET optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT round(toDecimal32(2, 2) * 1.2, 6);
SELECT round(toDecimal64(0.5, 2) * 20.33, 6);
SELECT round(0.00001 * toDecimal32(12, 2), 6);
SELECT round(30.033 * toDecimal32(5, 1), 6);

CREATE TABLE IF NOT EXISTS test01603 (
    f64 Float64,
    d Decimal64(3) DEFAULT toDecimal32(f64, 3),
    f32 Float32 DEFAULT f64
) ENGINE=MergeTree() ORDER BY f32;

INSERT INTO test01603(f64) SELECT 1 / (number + 1) FROM system.numbers LIMIT 1000;

SELECT round(sum(d * 1.1), 6) FROM test01603;
SELECT round(sum(8.01 * d), 6) FROM test01603;

SELECT round(sum(f64 * toDecimal64(80, 2)), 6) FROM test01603;
SELECT round(sum(toDecimal64(40, 2) * f32), 6) FROM test01603;
SELECT round(sum(f64 * toDecimal64(0.1, 2)), 6) FROM test01603;
SELECT round(sum(toDecimal64(0.3, 2) * f32), 6) FROM test01603;

SELECT round(sum(f64 * d), 6) FROM test01603;
SELECT round(sum(d * f64), 6) FROM test01603;
SELECT round(sum(f32 * d), 6) FROM test01603;
SELECT round(sum(d * f32), 6) FROM test01603;

DROP TABLE IF EXISTS test01603;
