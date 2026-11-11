DROP TABLE IF EXISTS decimal_in_float_test;

CREATE TABLE decimal_in_float_test ( `a` Decimal(18, 0), `b` Decimal(36, 2) ) ENGINE = Memory;
INSERT INTO decimal_in_float_test VALUES ('33', '44.44');

SELECT toDecimal32(1.555,3) IN (1.5551);
SELECT toDecimal32(1.555,3) IN (1.5551,1.555);
SELECT toDecimal32(1.555,3) IN (1.5551,1.555000);
SELECT toDecimal32(1.555,3) IN (1.550,1.5);

SELECT toDecimal64(1.555,3) IN (1.5551);
SELECT toDecimal64(1.555,3) IN (1.5551,1.555);
SELECT toDecimal64(1.555,3) IN (1.5551,1.555000);
SELECT toDecimal64(1.555,3) IN (1.550,1.5);

SELECT toDecimal128(1.555,3) IN (1.5551);
SELECT toDecimal128(1.555,3) IN (1.5551,1.555);
SELECT toDecimal128(1.555,3) IN (1.5551,1.555000);
SELECT toDecimal128(1.555,3) IN (1.550,1.5);

SELECT toDecimal256(1.555,3) IN (1.5551);
SELECT toDecimal256(1.555,3) IN (1.5551,1.555);
SELECT toDecimal256(1.555,3) IN (1.5551,1.555000);
SELECT toDecimal256(1.555,3) IN (1.550,1.5);


SELECT count() == 1 FROM decimal_in_float_test WHERE a IN (33);
SELECT count() == 1 FROM decimal_in_float_test WHERE a IN (33.0);
SELECT count() == 1 FROM decimal_in_float_test WHERE a NOT IN (33.333);
SELECT count() == 1 FROM decimal_in_float_test WHERE b IN (44.44);
SELECT count() == 1 FROM decimal_in_float_test WHERE b NOT IN (44.4,44.444);

SET enable_analyzer = 1;


SELECT toDecimal32(1.555,3) IN (1.5551);
SELECT toDecimal32(1.555,3) IN (1.5551,1.555);
SELECT toDecimal32(1.555,3) IN (1.5551,1.555000);
SELECT toDecimal32(1.555,3) IN (1.550,1.5);

SELECT toDecimal64(1.555,3) IN (1.5551);
SELECT toDecimal64(1.555,3) IN (1.5551,1.555);
SELECT toDecimal64(1.555,3) IN (1.5551,1.555000);
SELECT toDecimal64(1.555,3) IN (1.550,1.5);

SELECT toDecimal128(1.555,3) IN (1.5551);
SELECT toDecimal128(1.555,3) IN (1.5551,1.555);
SELECT toDecimal128(1.555,3) IN (1.5551,1.555000);
SELECT toDecimal128(1.555,3) IN (1.550,1.5);

SELECT toDecimal256(1.555,3) IN (1.5551);
SELECT toDecimal256(1.555,3) IN (1.5551,1.555);
SELECT toDecimal256(1.555,3) IN (1.5551,1.555000);
SELECT toDecimal256(1.555,3) IN (1.550,1.5);


SELECT count() == 1 FROM decimal_in_float_test WHERE a IN (33);
SELECT count() == 1 FROM decimal_in_float_test WHERE a IN (33.0);
SELECT count() == 1 FROM decimal_in_float_test WHERE a NOT IN (33.333);
SELECT count() == 1 FROM decimal_in_float_test WHERE b IN (44.44);
SELECT count() == 1 FROM decimal_in_float_test WHERE b NOT IN (44.4,44.444);

DROP TABLE IF EXISTS decimal_in_float_test;
