select CAST(1.0, 'Decimal(15,2)') > CAST(1, 'Float64');
select CAST(1.0, 'Decimal(15,2)') = CAST(1, 'Float64');
select CAST(1.0, 'Decimal(15,2)') < CAST(1, 'Float64');
select CAST(1.0, 'Decimal(15,2)') != CAST(1, 'Float64');
select CAST(1.0, 'Decimal(15,2)') > CAST(-1, 'Float64');
select CAST(1.0, 'Decimal(15,2)') = CAST(-1, 'Float64');
select CAST(1.0, 'Decimal(15,2)') < CAST(-1, 'Float64');
select CAST(1.0, 'Decimal(15,2)') != CAST(-1, 'Float64');
select CAST(1.0, 'Decimal(15,2)') > CAST(1, 'Float32');
select CAST(1.0, 'Decimal(15,2)') = CAST(1, 'Float32');
select CAST(1.0, 'Decimal(15,2)') < CAST(1, 'Float32');
select CAST(1.0, 'Decimal(15,2)') != CAST(1, 'Float32');
select CAST(1.0, 'Decimal(15,2)') > CAST(-1, 'Float32');
select CAST(1.0, 'Decimal(15,2)') = CAST(-1, 'Float32');
select CAST(1.0, 'Decimal(15,2)') < CAST(-1, 'Float32');
select CAST(1.0, 'Decimal(15,2)') != CAST(-1, 'Float32');

SELECT toDecimal32('11.00', 2) > 1.;

SELECT 0.1000000000000000055511151231257827021181583404541015625::Decimal256(70) = 0.1;

DROP TABLE IF EXISTS t;

CREATE TABLE t
(
	d1 Decimal32(5),
	d2 Decimal64(10),
	d3 Decimal128(30),
	d4 Decimal256(50),
	f1 Float32,
	f2 Float32
)ENGINE = Memory;

INSERT INTO t values (-1.5, -1.5, -1.5, -1.5, 1.5, 1.5);
INSERT INTO t values (1.5, 1.5, 1.5, 1.5, -1.5, -1.5);

SELECT d1 > f1 FROM t ORDER BY f1;
SELECT d2 > f1 FROM t ORDER BY f1;
SELECT d3 > f1 FROM t ORDER BY f1;
SELECT d4 > f1 FROM t ORDER BY f1;

SELECT d1 > f2 FROM t ORDER BY f2;
SELECT d2 > f2 FROM t ORDER BY f2;
SELECT d3 > f2 FROM t ORDER BY f2;
SELECT d4 > f2 FROM t ORDER BY f2;

DROP TABLE t;
