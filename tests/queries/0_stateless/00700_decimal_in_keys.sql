DROP TABLE IF EXISTS decimal;

CREATE TABLE IF NOT EXISTS decimal
(
    d1 DECIMAL(9, 8),
    d2 DECIMAL(18, 8),
    d3 DECIMAL(38, 8)
)
ENGINE = MergeTree
PARTITION BY toInt32(d1)
ORDER BY (d2, d3);

INSERT INTO decimal (d1, d2, d3) VALUES (4.2, 4.2, 4.2);

SELECT count() FROM decimal WHERE d1 =  toDecimal32('4.2', 8);
SELECT count() FROM decimal WHERE d1 != toDecimal32('4.2', 8);
SELECT count() FROM decimal WHERE d1 <  toDecimal32('4.2', 8);
SELECT count() FROM decimal WHERE d1 >  toDecimal32('4.2', 8);
SELECT count() FROM decimal WHERE d1 <= toDecimal32('4.2', 8);
SELECT count() FROM decimal WHERE d1 >= toDecimal32('4.2', 8);

INSERT INTO decimal (d1, d2, d3)
    SELECT toDecimal32(number % 10, 8), toDecimal64(number, 8), toDecimal128(number, 8) FROM system.numbers LIMIT 50;

SELECT count() FROM decimal WHERE d1 = 1;
SELECT * FROM decimal WHERE d1 > 5 AND d2 < 30 ORDER BY d2 DESC;
SELECT * FROM decimal WHERE d1 IN(1, 3) ORDER BY d2;

DROP TABLE decimal;
