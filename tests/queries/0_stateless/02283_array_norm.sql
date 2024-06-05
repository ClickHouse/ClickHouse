SELECT L1Norm([1, 2, 3]);
SELECT L2Norm([3., 4., 5.]);
SELECT L2SquaredNorm([3., 4., 5.]);
SELECT LpNorm([3., 4., 5.], 1.1);
SELECT LinfNorm([0, 0, 2]);

-- Overflows
WITH CAST([-547274980, 1790553898, 1981517754, 1908431500, 1352428565, -573412550, -552499284, 2096941042], 'Array(Int32)') AS a
SELECT
    L1Norm(a),
    L2Norm(a),
    L2SquaredNorm(a),
    LpNorm(a,1),
    LpNorm(a,2),
    LpNorm(a,3.14),
    LinfNorm(a);

DROP TABLE IF EXISTS vec1;
DROP TABLE IF EXISTS vec1f;
DROP TABLE IF EXISTS vec1d;
CREATE TABLE vec1 (id UInt64, v Array(UInt8)) ENGINE = Memory;
CREATE TABLE vec1f (id UInt64, v Array(Float32)) ENGINE = Memory;
CREATE TABLE vec1d (id UInt64, v Array(Float64)) ENGINE = Memory;
INSERT INTO vec1 VALUES (1, [3, 4]), (2, [2]), (3, [3, 3, 3]), (4, NULL), (5, range(7, 27)), (6, range(3, 103));
INSERT INTO vec1f VALUES (1, [3, 4]), (2, [2]), (3, [3, 3, 3]), (4, NULL), (5, range(7, 27)), (6, range(3, 103));
INSERT INTO vec1d VALUES (1, [3, 4]), (2, [2]), (3, [3, 3, 3]), (4, NULL), (5, range(7, 27)), (6, range(3, 103));

SELECT id, L1Norm(v), L2Norm(v), L2SquaredNorm(v), LpNorm(v, 2.7), LinfNorm(v) FROM vec1;
SELECT id, L1Norm(materialize([5., 6.])) FROM vec1;

SELECT id, L1Norm(v), L2Norm(v), L2SquaredNorm(v), LpNorm(v, 2.7), LinfNorm(v) FROM vec1f;
SELECT id, L1Norm(materialize([5., 6.])) FROM vec1f;

SELECT id, L1Norm(v), L2Norm(v), L2SquaredNorm(v), LpNorm(v, 2.7), LinfNorm(v) FROM vec1d;
SELECT id, L1Norm(materialize([5., 6.])) FROM vec1d;

SELECT L1Norm(1, 2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT LpNorm([1,2]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT LpNorm([1,2], -3.4); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT LpNorm([1,2], 'aa'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT LpNorm([1,2], [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT LpNorm([1,2], materialize(3.14)); -- { serverError ILLEGAL_COLUMN }

DROP TABLE vec1;
DROP TABLE vec1f;
DROP TABLE vec1d;
