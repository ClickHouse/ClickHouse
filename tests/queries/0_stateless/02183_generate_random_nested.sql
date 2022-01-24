SET optimize_functions_to_subcolumns = 0;
SET flatten_nested = 0;

SELECT * FROM generateRandom('point Nested(x UInt64, t Nested(z UInt64, s String))', 1, 10, 2) LIMIT 1;
SELECT point.x FROM generateRandom('point Nested(x UInt64, t Nested(z UInt64, s String))', 1, 10, 2) LIMIT 1;
SELECT point.x, point.t FROM generateRandom('point Nested(x UInt64, t Nested(z UInt64, s String))', 1, 10, 2) LIMIT 1;
SELECT point.t.z, point.t.s FROM generateRandom('point Nested(x UInt64, t Nested(z UInt64, s String))', 1, 10, 2) LIMIT 1;
SELECT point.size0, point.x FROM generateRandom('point Nested(x UInt64, t Nested(z UInt64, s String))', 1, 10, 2) LIMIT 1;

SET flatten_nested = 1;

SELECT * FROM generateRandom('point Nested(x UInt64, t Nested(z UInt64, s String))', 1, 10, 2) LIMIT 1;
SELECT point.x FROM generateRandom('point Nested(x UInt64, t Nested(z UInt64, s String))', 1, 10, 2) LIMIT 1;
SELECT point.x, point.t FROM generateRandom('point Nested(x UInt64, t Nested(z UInt64, s String))', 1, 10, 2) LIMIT 1;
SELECT point.t.z, point.t.s FROM generateRandom('point Nested(x UInt64, t Nested(z UInt64, s String))', 1, 10, 2) LIMIT 1;
