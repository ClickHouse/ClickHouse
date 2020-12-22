SELECT toDecimal32(2, 2) * 1.2;
SELECT toDecimal64(0.5, 2) * 20.33;
SELECT 0.00001 * toDecimal32(12, 2);
SELECT 30.033 * toDecimal32(5, 1);

CREATE TABLE IF NOT EXISTS test01603 (
    f64 Float64,
    d Decimal64(3) DEFAULT toDecimal32(f64, 3),
    f32 Float32 DEFAULT f64
) ENGINE=MergeTree() ORDER BY f32;

INSERT INTO test01603(f64) SELECT 1 / (number + 1) FROM system.numbers LIMIT 1000;

SELECT sum(d * 1.1) FROM test01603;
SELECT sum(8.01 * d) FROM test01603;

SELECT sum(f64 * toDecimal64(80, 2)) FROM test01603;
SELECT sum(toDecimal64(40, 2) * f32) FROM test01603;
SELECT sum(f64 * toDecimal64(0.1, 2)) FROM test01603;
SELECT sum(toDecimal64(0.3, 2) * f32) FROM test01603;

SELECT sum(f64 * d) FROM test01603;
SELECT sum(d * f64) FROM test01603;
SELECT sum(f32 * d) FROM test01603;
SELECT sum(d * f32) FROM test01603;

DROP TABLE IF EXISTS test01603;
