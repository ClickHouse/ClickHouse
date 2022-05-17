SELECT arrayL1Norm([1, 2, 3]);
SELECT arrayL2Norm([3., 4., 5.]);
SELECT arrayLinfNorm([0, 0, 2]);

DROP TABLE IF EXISTS vec1;
DROP TABLE IF EXISTS vec1f;
DROP TABLE IF EXISTS vec1d;
CREATE TABLE vec1 (id UInt64, v Array(UInt8)) ENGINE = Memory;
CREATE TABLE vec1f (id UInt64, v Array(Float32)) ENGINE = Memory;
CREATE TABLE vec1d (id UInt64, v Array(Float64)) ENGINE = Memory;
INSERT INTO vec1 VALUES (1, [3, 4]), (2, [2]), (3, [3, 3, 3]), (4, NULL);
INSERT INTO vec1f VALUES (1, [3, 4]), (2, [2]), (3, [3, 3, 3]), (4, NULL);
INSERT INTO vec1d VALUES (1, [3, 4]), (2, [2]), (3, [3, 3, 3]), (4, NULL);

SELECT id, arrayL2Norm(v) FROM vec1;
SELECT id, arrayL1Norm(materialize([5., 6.])) FROM vec1;

SELECT id, arrayL2Norm(v) FROM vec1f;
SELECT id, arrayL1Norm(materialize([5., 6.])) FROM vec1f;

SELECT id, arrayL2Norm(v) FROM vec1d;
SELECT id, arrayL1Norm(materialize([5., 6.])) FROM vec1d;

SELECT arrayL1Norm((1, 2,)); -- { serverError 43 }

DROP TABLE vec1;
DROP TABLE vec1f;
DROP TABLE vec1d;
