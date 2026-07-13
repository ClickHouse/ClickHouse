SET join_algorithm = 'hash';

SELECT L1Distance([0, 0, 0], [1, 2, 3]);
SELECT L2Distance([1, 2, 3], [0, 0, 0]);
SELECT L2SquaredDistance([1, 2, 3], [0, 0, 0]);
SELECT LpDistance([1, 2, 3], [0, 0, 0], 3.5);
SELECT LinfDistance([1, 2, 3], [0, 0, 0]);
SELECT cosineDistance([1, 2, 3], [3, 5, 7]);

SELECT L2Distance([1, 2, 3], NULL);
SELECT L2SquaredDistance([1, 2, 3], NULL);
SELECT cosineDistance([1, 2, 3], [0, 0, 0]);

-- Overflows
WITH CAST([-547274980, 1790553898, 1981517754, 1908431500, 1352428565, -573412550, -552499284, 2096941042], 'Array(Int32)') AS a
SELECT
    L1Distance(a, a),
    L2Distance(a, a),
    L2SquaredDistance(a, a),
    LinfDistance(a, a),
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
SELECT L2SquaredDistance(v, [0, 0, 0]) FROM vec1;
SELECT LpDistance(v, [0, 0, 0], 3.14) FROM vec1;
SELECT LinfDistance([5, 4, 3], v) FROM vec1;
SELECT cosineDistance([3, 2, 1], v) FROM vec1;
SELECT LinfDistance(v, materialize([0, -2, 0])) FROM vec1;
SELECT cosineDistance(v, materialize([1., 1., 1.])) FROM vec1;

INSERT INTO vec2 VALUES (1, [100, 200, 0]), (2, [888, 777, 666]), (3, range(1, 35, 1)), (4, range(3, 37, 1)), (5, range(1, 135, 1)), (6, range(3, 137, 1));
SELECT
    v1.id,
    v2.id,
    L1Distance(v1.v, v2.v),
    LinfDistance(v1.v, v2.v),
    LpDistance(v1.v, v2.v, 3.1),
    L2Distance(v1.v, v2.v),
    L2SquaredDistance(v1.v, v2.v),
    cosineDistance(v1.v, v2.v)
FROM vec2 v1, vec2 v2
WHERE length(v1.v) == length(v2.v)
ORDER BY ALL;

INSERT INTO vec2f VALUES (1, [100, 200, 0]), (2, [888, 777, 666]), (3, range(1, 35, 1)), (4, range(3, 37, 1)), (5, range(1, 135, 1)), (6, range(3, 137, 1));
SELECT
    v1.id,
    v2.id,
    L1Distance(v1.v, v2.v),
    LinfDistance(v1.v, v2.v),
    LpDistance(v1.v, v2.v, 3),
    L2Distance(v1.v, v2.v),
    L2SquaredDistance(v1.v, v2.v),
    cosineDistance(v1.v, v2.v)
FROM vec2f v1, vec2f v2
WHERE length(v1.v) == length(v2.v)
ORDER BY ALL;

INSERT INTO vec2d VALUES (1, [100, 200, 0]), (2, [888, 777, 666]), (3, range(1, 35, 1)), (4, range(3, 37, 1)), (5, range(1, 135, 1)), (6, range(3, 137, 1));
SELECT
    v1.id,
    v2.id,
    L1Distance(v1.v, v2.v),
    LinfDistance(v1.v, v2.v),
    LpDistance(v1.v, v2.v, 3),
    L2Distance(v1.v, v2.v),
    L2SquaredDistance(v1.v, v2.v),
    cosineDistance(v1.v, v2.v)
FROM vec2d v1, vec2d v2
WHERE length(v1.v) == length(v2.v)
ORDER BY ALL;

SELECT
    v1.id,
    v2.id,
    L1Distance(v1.v, v2.v),
    LinfDistance(v1.v, v2.v),
    LpDistance(v1.v, v2.v, 3),
    L2Distance(v1.v, v2.v),
    L2SquaredDistance(v1.v, v2.v),
    cosineDistance(v1.v, v2.v)
FROM vec2f v1, vec2d v2
WHERE length(v1.v) == length(v2.v)
ORDER BY ALL;

SELECT L1Distance([0, 0], [1]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT L2Distance([1, 2], (3,4)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT L2SquaredDistance([1, 2], (3,4)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT LpDistance([1, 2], [3,4]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT LpDistance([1, 2], [3,4], -1.); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT LpDistance([1, 2], [3,4], 'aaa'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT LpDistance([1, 2], [3,4], materialize(2.7)); -- { serverError ILLEGAL_COLUMN }

DROP TABLE vec1;
DROP TABLE vec2;
DROP TABLE vec2f;
DROP TABLE vec2d;

-- Queries which trigger manually vectorized implementation

SELECT L2Distance(
    [toFloat32(0.0), toFloat32(1.0), toFloat32(2.0), toFloat32(3.0), toFloat32(4.0), toFloat32(5.0), toFloat32(6.0), toFloat32(7.0), toFloat32(8.0), toFloat32(9.0), toFloat32(10.0), toFloat32(11.0), toFloat32(12.0), toFloat32(13.0), toFloat32(14.0), toFloat32(15.0), toFloat32(16.0), toFloat32(17.0), toFloat32(18.0), toFloat32(19.0), toFloat32(20.0), toFloat32(21.0), toFloat32(22.0), toFloat32(23.0), toFloat32(24.0), toFloat32(25.0), toFloat32(26.0), toFloat32(27.0), toFloat32(28.0), toFloat32(29.0), toFloat32(30.0), toFloat32(31.0), toFloat32(32.0), toFloat32(33.0)],
    materialize([toFloat32(1.0), toFloat32(2.0), toFloat32(3.0), toFloat32(4.0), toFloat32(5.0), toFloat32(6.0), toFloat32(7.0), toFloat32(8.0), toFloat32(9.0), toFloat32(10.0), toFloat32(11.0), toFloat32(12.0), toFloat32(13.0), toFloat32(14.0), toFloat32(15.0), toFloat32(16.0), toFloat32(17.0), toFloat32(18.0), toFloat32(19.0), toFloat32(20.0), toFloat32(21.0), toFloat32(22.0), toFloat32(23.0), toFloat32(24.0), toFloat32(25.0), toFloat32(26.0), toFloat32(27.0), toFloat32(28.0), toFloat32(29.0), toFloat32(30.0), toFloat32(31.0), toFloat32(32.0), toFloat32(33.0), toFloat32(34.0)]));

SELECT cosineDistance(
    [toFloat32(0.0), toFloat32(1.0), toFloat32(2.0), toFloat32(3.0), toFloat32(4.0), toFloat32(5.0), toFloat32(6.0), toFloat32(7.0), toFloat32(8.0), toFloat32(9.0), toFloat32(10.0), toFloat32(11.0), toFloat32(12.0), toFloat32(13.0), toFloat32(14.0), toFloat32(15.0), toFloat32(16.0), toFloat32(17.0), toFloat32(18.0), toFloat32(19.0), toFloat32(20.0), toFloat32(21.0), toFloat32(22.0), toFloat32(23.0), toFloat32(24.0), toFloat32(25.0), toFloat32(26.0), toFloat32(27.0), toFloat32(28.0), toFloat32(29.0), toFloat32(30.0), toFloat32(31.0), toFloat32(32.0), toFloat32(33.0)],
    materialize([toFloat32(1.0), toFloat32(2.0), toFloat32(3.0), toFloat32(4.0), toFloat32(5.0), toFloat32(6.0), toFloat32(7.0), toFloat32(8.0), toFloat32(9.0), toFloat32(10.0), toFloat32(11.0), toFloat32(12.0), toFloat32(13.0), toFloat32(14.0), toFloat32(15.0), toFloat32(16.0), toFloat32(17.0), toFloat32(18.0), toFloat32(19.0), toFloat32(20.0), toFloat32(21.0), toFloat32(22.0), toFloat32(23.0), toFloat32(24.0), toFloat32(25.0), toFloat32(26.0), toFloat32(27.0), toFloat32(28.0), toFloat32(29.0), toFloat32(30.0), toFloat32(31.0), toFloat32(32.0), toFloat32(33.0), toFloat32(34.0)]));

SELECT L2Distance(
    [toFloat64(0.0), toFloat64(1.0), toFloat64(2.0), toFloat64(3.0), toFloat64(4.0), toFloat64(5.0), toFloat64(6.0), toFloat64(7.0), toFloat64(8.0), toFloat64(9.0), toFloat64(10.0), toFloat64(11.0), toFloat64(12.0), toFloat64(13.0), toFloat64(14.0), toFloat64(15.0), toFloat64(16.0), toFloat64(17.0), toFloat64(18.0), toFloat64(19.0), toFloat64(20.0), toFloat64(21.0), toFloat64(22.0), toFloat64(23.0), toFloat64(24.0), toFloat64(25.0), toFloat64(26.0), toFloat64(27.0), toFloat64(28.0), toFloat64(29.0), toFloat64(30.0), toFloat64(31.0), toFloat64(32.0), toFloat64(33.0)],
    materialize([toFloat64(1.0), toFloat64(2.0), toFloat64(3.0), toFloat64(4.0), toFloat64(5.0), toFloat64(6.0), toFloat64(7.0), toFloat64(8.0), toFloat64(9.0), toFloat64(10.0), toFloat64(11.0), toFloat64(12.0), toFloat64(13.0), toFloat64(14.0), toFloat64(15.0), toFloat64(16.0), toFloat64(17.0), toFloat64(18.0), toFloat64(19.0), toFloat64(20.0), toFloat64(21.0), toFloat64(22.0), toFloat64(23.0), toFloat64(24.0), toFloat64(25.0), toFloat64(26.0), toFloat64(27.0), toFloat64(28.0), toFloat64(29.0), toFloat64(30.0), toFloat64(31.0), toFloat64(32.0), toFloat64(33.0), toFloat64(34.0)]));

SELECT cosineDistance(
    [toFloat64(0.0), toFloat64(1.0), toFloat64(2.0), toFloat64(3.0), toFloat64(4.0), toFloat64(5.0), toFloat64(6.0), toFloat64(7.0), toFloat64(8.0), toFloat64(9.0), toFloat64(10.0), toFloat64(11.0), toFloat64(12.0), toFloat64(13.0), toFloat64(14.0), toFloat64(15.0), toFloat64(16.0), toFloat64(17.0), toFloat64(18.0), toFloat64(19.0), toFloat64(20.0), toFloat64(21.0), toFloat64(22.0), toFloat64(23.0), toFloat64(24.0), toFloat64(25.0), toFloat64(26.0), toFloat64(27.0), toFloat64(28.0), toFloat64(29.0), toFloat64(30.0), toFloat64(31.0), toFloat64(32.0), toFloat64(33.0)],
    materialize([toFloat64(1.0), toFloat64(2.0), toFloat64(3.0), toFloat64(4.0), toFloat64(5.0), toFloat64(6.0), toFloat64(7.0), toFloat64(8.0), toFloat64(9.0), toFloat64(10.0), toFloat64(11.0), toFloat64(12.0), toFloat64(13.0), toFloat64(14.0), toFloat64(15.0), toFloat64(16.0), toFloat64(17.0), toFloat64(18.0), toFloat64(19.0), toFloat64(20.0), toFloat64(21.0), toFloat64(22.0), toFloat64(23.0), toFloat64(24.0), toFloat64(25.0), toFloat64(26.0), toFloat64(27.0), toFloat64(28.0), toFloat64(29.0), toFloat64(30.0), toFloat64(31.0), toFloat64(32.0), toFloat64(33.0), toFloat64(34.0)]));
