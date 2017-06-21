DROP TABLE IF EXISTS test.nested;
CREATE TABLE test.nested (nest Nested(x UInt8, y UInt8)) ENGINE = Memory;
INSERT INTO test.nested VALUES ([1, 2, 3], [4, 5, 6]);

SELECT nx FROM test.nested ARRAY JOIN nest.x AS nx, nest.y AS ny WHERE notEmpty(nest.y);
SELECT 1 FROM test.nested ARRAY JOIN nest.x AS nx, nest.y AS ny WHERE notEmpty(nest.y);
SELECT nx, ny FROM test.nested ARRAY JOIN nest.x AS nx, nest.y AS ny WHERE notEmpty(nest.y);
SELECT nx FROM test.nested ARRAY JOIN nest.x AS nx, nest.y AS ny WHERE notEmpty(nest.x);
SELECT nx, nest.y FROM test.nested ARRAY JOIN nest.x AS nx, nest.y AS ny;
SELECT nx, ny, nest.x, nest.y FROM test.nested ARRAY JOIN nest.x AS nx, nest.y AS ny;

DROP TABLE test.nested;
