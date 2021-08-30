SELECT tupleHammingDistance(tuple(1), tuple(1));
SELECT tupleHammingDistance((1, 3), (1, 2));

SELECT (1, 2) + tupleMultiply((3, 4), (5, 1)) - (6, 3);
SELECT vectorDifference(tuplePlus((1, 2), (3, 4)), (5, 6));
SELECT tupleMinus(vectorSum(tupleMultiply((1, 2), (3, 4)), (5, 6)), (31, 41));
SELECT tupleDivide((5, 8, 11), (-2, 2, 4));
SELECT tuple(1) + tuple(2);

SELECT tupleNegate((1, 0, 3.5));

SELECT tupleMultiplyByNumber((1, 2, 3), 0.5);
SELECT tupleDivideByNumber((1, 2.5, 3), 0.5);
SELECT tupleMultiplyByNumber(tuple(1), 1);
SELECT tupleDivideByNumber(tuple(1), 1);

SELECT tuple(1, 2, 3) * tuple(2, 3, 4);
SELECT dotProduct((-1, 2, 3.002), (2, 3.4, 4));
SELECT scalarProduct(tuple(1), tuple(0));

SELECT L1Norm((-1, 2, -3));
SELECT L1Norm((-1, 2.5, -3.6));
SELECT L2Norm((1, 1.0));
SELECT L2Norm((-12, 5));

SELECT max2(1, 1.5);
SELECT min2(-1, -3);
SELECT LinfNorm((1, -2.3, 1.7));

SELECT LpNorm(tuple(-1), 3);
SELECT LpNorm(tuple(-1.1), 3);
SELECT LpNorm((95800, 217519, 414560), 4);
SELECT LpNorm((13, -84.4, 91, 63.1), 2) = L2Norm(tuple(13, -84.4, 91, 63.1));
SELECT LpNorm((13, -84.4, 91, 63.1), 1) = L1Norm(tuple(13, -84.4, 91, 63.1));
SELECT LpNorm((-1, -2), 11);

SELECT L1Distance((1, 2, 3), (2, 3, 1));
SELECT L2Distance((1, 1), (3, -1));
SELECT LinfDistance((1, 1), (1, 2));
SELECT L2Distance((5, 5), (5, 5));
SELECT LpDistance((1800, 1900), (18, 59), 12) - LpDistance(tuple(-22), tuple(1900), 12);

SELECT L1Normalize((1, -4));
SELECT L2Normalize((3, 4));
SELECT LinfNormalize((5, -5, 5.0));
SELECT LpNormalize((1, 1.98734075466445795857), 5);
