SELECT arrayL1Norm([1, 2, 3]);
SELECT arrayL2Norm([3., 4., 5.]);
SELECT arrayLinfNorm([0, 0, 2]);

DROP TABLE IF EXISTS vec;
CREATE TABLE vec (id UInt64, v Array(UInt8)) ENGINE = Memory;
INSERT INTO vec VALUES (1, [3, 4]), (2, [2]), (3, [3, 3, 3]), (4, NULL);

SELECT id, arrayL2Norm(v) FROM vec;
SELECT id, arrayL1Norm(materialize([5., 6.])) FROM vec;
SELECT arrayL1Norm((1, 2,)); -- { serverError 43 }

DROP TABLE vec;
