-- A lambda APPLY transformer over JOIN ... USING must name each result after the matched column,
-- not after the lambda parameter. Identical names made the outer `*` shadow one column with another.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS al;
DROP TABLE IF EXISTS ar;
DROP TABLE IF EXISTS sl;
DROP TABLE IF EXISTS sr;
DROP TABLE IF EXISTS aj;

CREATE TABLE al (id UInt64, g UInt16, v Int64)  ENGINE = MergeTree ORDER BY id;
CREATE TABLE ar (id UInt64, g UInt16, w String) ENGINE = MergeTree ORDER BY id;
INSERT INTO al VALUES (100, 1, 10);
INSERT INTO ar VALUES (100, 1, 'a');

CREATE TABLE sl (t Tuple(a UInt64), v Int64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE sr (t Tuple(a UInt64), w String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO sl VALUES ((100), 10);
INSERT INTO sr VALUES ((100), 'a');

CREATE TABLE aj (id UInt64, a1 Array(UInt8), a2 Array(UInt8)) ENGINE = MergeTree ORDER BY id;
INSERT INTO aj VALUES (1, [10], [20]);

SELECT '-- unqualified * over JOIN USING: every USING column keeps its own name';
SELECT * APPLY (x -> toString(x)) FROM al JOIN ar USING (id, g) FORMAT TSVWithNames;

SELECT '-- wrapping in a passthrough SELECT * must not swap values between USING columns';
SELECT * FROM (SELECT * APPLY (x -> toString(x)) FROM al JOIN ar USING (id, g)) FORMAT TSVWithNames;

SELECT '-- USING columns of different types must not collide into one name';
SELECT * APPLY (x -> toTypeName(x)) FROM al JOIN ar USING (id, g) FORMAT TSVWithNames;

SELECT '-- COLUMNS list form';
SELECT COLUMNS(id, g) APPLY (x -> toString(x)) FROM al JOIN ar USING (id, g) FORMAT TSVWithNames;

SELECT '-- ARRAY JOIN columns are a third producer of an unregistered matched node';
SELECT * APPLY (x -> toString(x)) FROM aj ARRAY JOIN a1, a2 FORMAT TSVWithNames;
SELECT * FROM (SELECT * APPLY (x -> toString(x)) FROM aj ARRAY JOIN a1, a2) FORMAT TSVWithNames;

SELECT '-- FULL JOIN promotes the USING column to a function of both sides';
SELECT * APPLY (x -> toString(x)) FROM al FULL JOIN ar USING (id, g) ORDER BY 1 FORMAT TSVWithNames;

SELECT '-- join_use_nulls';
SELECT * APPLY (x -> toString(x)) FROM al LEFT JOIN ar USING (id, g) SETTINGS join_use_nulls = 1 FORMAT TSVWithNames;

SELECT '-- a single USING column cannot collide, but was still named after the parameter';
SELECT * APPLY (x -> toString(x)) FROM al JOIN ar USING (id) FORMAT TSVWithNames;

SELECT '-- subcolumn USING key';
SELECT * APPLY (x -> toString(x)) FROM sl JOIN sr USING (t.a) FORMAT TSVWithNames;

SELECT '-- chained lambda APPLY transformers';
SELECT * APPLY (x -> toString(x)) APPLY (y -> length(y)) FROM al JOIN ar USING (id, g) FORMAT TSVWithNames;

SELECT '-- the function form of APPLY was already correct and must not change';
SELECT * APPLY toString FROM al JOIN ar USING (id, g) FORMAT TSVWithNames;

SELECT '-- a qualified column selected next to the matcher keeps its own name, in either order';
SELECT al.id, * APPLY (x -> toString(x)) FROM al JOIN ar USING (id, g) FORMAT TSVWithNames;
SELECT * APPLY (x -> toString(x)), al.id FROM al JOIN ar USING (id, g) FORMAT TSVWithNames;

SELECT '-- an ordinary higher-order lambda still shows its parameter name';
SELECT arrayMap(x -> x + 1, [1, 2]) FROM al JOIN ar USING (id, g) FORMAT TSVWithNames;

DROP TABLE al;
DROP TABLE ar;
DROP TABLE sl;
DROP TABLE sr;
DROP TABLE aj;
