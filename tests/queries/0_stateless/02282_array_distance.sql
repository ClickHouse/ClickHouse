SELECT arrayL1Distance([0, 0, 0], [1, 2, 3]);
SELECT arrayL2Distance([1, 2, 3], [0, 0, 0]);
SELECT arrayLinfDistance([1, 2, 3], [0, 0, 0]);
SELECT arrayCosineDistance([1, 2, 3], [3, 5, 7]);

SELECT arrayL2Distance([1, 2, 3], NULL);
SELECT arrayCosineDistance([1, 2, 3], [0, 0, 0]);

DROP TABLE IF EXISTS vec1;
DROP TABLE IF EXISTS vec2;
DROP TABLE IF EXISTS vec2f;
DROP TABLE IF EXISTS vec2d;
CREATE TABLE vec1 (id UInt64, v Array(UInt8)) ENGINE = Memory;
CREATE TABLE vec2 (id UInt64, v Array(Int64)) ENGINE = Memory;
CREATE TABLE vec2f (id UInt64, v Array(Float32)) ENGINE = Memory;
CREATE TABLE vec2d (id UInt64, v Array(Float64)) ENGINE = Memory;

INSERT INTO vec1 VALUES (1, [3, 4, 5]), (2, [2, 4, 8]), (3, [7, 7, 7]);
SELECT arrayL1Distance(v, [0, 0, 0]) FROM vec1;
SELECT arrayL2Distance(v, [0, 0, 0]) FROM vec1;
SELECT arrayLinfDistance([5, 4, 3], v) FROM vec1;
SELECT arrayCosineDistance([3, 2, 1], v) FROM vec1;
SELECT arrayLinfDistance(v, materialize([0, -2, 0])) FROM vec1;
SELECT arrayCosineDistance(v, materialize([1., 1., 1.])) FROM vec1;

INSERT INTO vec2 VALUES (1, [100, 200, 0]), (2, [888, 777, 666]);
SELECT v1.id, v2.id, arrayL2Distance(v1.v, v2.v) as dist FROM vec1 v1, vec2 v2;

INSERT INTO vec2f VALUES (1, [100, 200, 0]), (2, [888, 777, 666]);
SELECT v1.id, v2.id, arrayL2Distance(v1.v, v2.v) as dist FROM vec1 v1, vec2f v2;

INSERT INTO vec2d VALUES (1, [100, 200, 0]), (2, [888, 777, 666]);
SELECT v1.id, v2.id, arrayL2Distance(v1.v, v2.v) as dist FROM vec1 v1, vec2d v2;

SELECT arrayL1Distance([0, 0], [1]); -- { serverError 190 }
SELECT arrayL2Distance((1, 2), (3,4)); -- { serverError 43 }

DROP TABLE vec1;
DROP TABLE vec2;
DROP TABLE vec2f;
DROP TABLE vec2d;
