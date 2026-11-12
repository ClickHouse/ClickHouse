-- Adding an element to a named Tuple must fill the new element of existing rows with the default value
-- of the element type (for Enum: the member with the lowest value), not with the zero of the underlying column.
-- https://github.com/ClickHouse/ClickHouse/issues/130899

SET mutations_sync = 2;

SELECT 'CAST';
SELECT CAST(tuple(5)::Tuple(a UInt8) AS Tuple(a UInt8, status Enum8('x' = 1, 'y' = 2))).status;

SELECT 'ALTER MODIFY COLUMN';
DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt32, attrs Tuple(a UInt8)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, tuple(5));
ALTER TABLE t MODIFY COLUMN attrs Tuple(a UInt8, status Enum8('x' = 1, 'y' = 2));
SELECT id, attrs.a, attrs.status FROM t;
OPTIMIZE TABLE t FINAL;
SELECT * FROM t;
DROP TABLE t;

SELECT 'INSERT SELECT';
DROP TABLE IF EXISTS old;
DROP TABLE IF EXISTS new;
CREATE TABLE old (id UInt32, attrs Tuple(a UInt8)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE new (id UInt32, attrs Tuple(a UInt8, status Enum8('x' = 1, 'y' = 2))) ENGINE = MergeTree ORDER BY id;
INSERT INTO old VALUES (2, tuple(7));
INSERT INTO new SELECT * FROM old;
SELECT attrs.status, count() FROM new GROUP BY attrs.status;
DROP TABLE old;
DROP TABLE new;

SELECT 'Array of Tuple';
DROP TABLE IF EXISTS ar;
CREATE TABLE ar (id UInt32, items Array(Tuple(a UInt8))) ENGINE = MergeTree ORDER BY id;
INSERT INTO ar VALUES (1, [tuple(5), tuple(6)]);
ALTER TABLE ar MODIFY COLUMN items Array(Tuple(a UInt8, status Enum8('x' = 1, 'y' = 2)));
SELECT items FROM ar;
DROP TABLE ar;

SELECT 'Enum without default zero member';
DROP TABLE IF EXISTS mz;
CREATE TABLE mz (id UInt32, attrs Tuple(a UInt8)) ENGINE = MergeTree ORDER BY id;
INSERT INTO mz VALUES (1, tuple(5));
ALTER TABLE mz MODIFY COLUMN attrs Tuple(a UInt8, status Enum8('m' = -3, 'n' = 0, 'o' = 4));
SELECT attrs.status FROM mz;
DROP TABLE mz;

SELECT 'Nested Tuple with Enum';
SELECT CAST(tuple(5)::Tuple(a UInt8) AS Tuple(a UInt8, t Tuple(e Enum8('x' = 1, 'y' = 2), n Nullable(UInt8), s String))).t;

SELECT 'Other element types keep their defaults';
SELECT CAST(tuple(5)::Tuple(a UInt8) AS Tuple(a UInt8, b Nullable(UInt8), c LowCardinality(String), d Array(UInt8), e DateTime64(3, 'UTC'))) SETTINGS session_timezone = 'UTC';
