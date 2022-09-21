SELECT L1Distance([0, 0, 0], [1, 2, 3]);
SELECT L2Distance([1, 2, 3], [0, 0, 0]);
SELECT LpDistance([1, 2, 3], [0, 0, 0], 3.5);
SELECT LinfDistance([1, 2, 3], [0, 0, 0]);
SELECT cosineDistance([1, 2, 3], [3, 5, 7]);

SELECT L2Distance([1, 2, 3], NULL);
SELECT cosineDistance([1, 2, 3], [0, 0, 0]);

-- Overflows
WITH CAST([-547274980, 1790553898, 1981517754, 1908431500, 1352428565, -573412550, -552499284, 2096941042], 'Array(Int32)') AS a
SELECT
    L1Distance(a,a),
    L2Distance(a,a),
    LinfDistance(a,a),
    cosineDistance(a, a);

DROP TABLE IF EXISTS vec1;
DROP TABLE IF EXISTS vec2;
DROP TABLE IF EXISTS vec2f;
DROP TABLE IF EXISTS vec2d;
CREATE TABLE vec1 (id UInt64, v Array(UInt8)) ENGINE = Memory;
CREATE TABLE vec2 (id UInt64, v Array(Int64)) ENGINE = Memory;
CREATE TABLE vec2f (id UInt64, v Array(Float32)) ENGINE = Memory;
CREATE TABLE vec2d (id UInt64, v Array(Float64)) ENGINE = Memory;

INSERT INTO vec1 VALUES (1, [3, 4, 5]), (2, [2, 4, 8]), (3, [7, 7, 7]);
SELECT L1Distance(v, [0, 0, 0]) FROM vec1;
SELECT L2Distance(v, [0, 0, 0]) FROM vec1;
SELECT LpDistance(v, [0, 0, 0], 3.14) FROM vec1;
SELECT LinfDistance([5, 4, 3], v) FROM vec1;
SELECT cosineDistance([3, 2, 1], v) FROM vec1;
SELECT LinfDistance(v, materialize([0, -2, 0])) FROM vec1;
SELECT cosineDistance(v, materialize([1., 1., 1.])) FROM vec1;

INSERT INTO vec2 VALUES (1, [100, 200, 0]), (2, [888, 777, 666]);
SELECT v1.id, v2.id, L2Distance(v1.v, v2.v) as dist FROM vec1 v1, vec2 v2;

INSERT INTO vec2f VALUES (1, [100, 200, 0]), (2, [888, 777, 666]);
SELECT v1.id, v2.id, L2Distance(v1.v, v2.v) as dist FROM vec1 v1, vec2f v2;

INSERT INTO vec2d VALUES (1, [100, 200, 0]), (2, [888, 777, 666]);
SELECT v1.id, v2.id, L2Distance(v1.v, v2.v) as dist FROM vec1 v1, vec2d v2;

SELECT L1Distance([0, 0], [1]); -- { serverError 190 }
SELECT L2Distance([1, 2], (3,4)); -- { serverError 43 }
SELECT LpDistance([1, 2], [3,4]); -- { serverError 42 }
SELECT LpDistance([1, 2], [3,4], -1.); -- { serverError 69 }
SELECT LpDistance([1, 2], [3,4], 'aaa'); -- { serverError 43 }
SELECT LpDistance([1, 2], [3,4], materialize(2.7)); -- { serverError 44 }

DROP TABLE vec1;
DROP TABLE vec2;
DROP TABLE vec2f;
DROP TABLE vec2d;
