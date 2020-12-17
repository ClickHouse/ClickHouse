SELECT toDecimal32(2, 2) * 1.2;
SELECT toDecimal64(0.5, 2) * 20.33;
SELECT 0.00001 * toDecimal32(12, 2);
SELECT 30.033 * toDecimal32(5, 1);

CREATE TABLE test01603 IF NOT EXISTS(
    f64 Float64,
    d Decimal64(3) DEFAULT toDecimal32(f64, 3),
    f32 Float32 DEFAULT f64
);

INSERT INTO test01603(f64) SELECT 1 / num FROM system.numbers LIMIT 1000;

SELECT sum(d * 1.1) FROM test01603;
SELECT sum(8.01 * d) FROM test01603;
SELECT sum(f64 * toDecimal64(80, 2)) FROM test01603;
SELECT sum(toDecimal64(0.4, 2) * f32) FROM test01603;

DROP TABLE test01603 IF EXISTS;
