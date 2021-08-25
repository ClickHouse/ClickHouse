SELECT tupleHammingDistance(tuple(1), tuple(1));
SELECT tupleHammingDistance(tuple(1, 3), tuple(1, 2));

SELECT tuple(1, 2) + tuple(3, 4) * tuple(5, 1) - tuple(6, 3);
SELECT vectorDifference(tuplePlus(tuple(1, 2), tuple(3, 4)), tuple(5, 6));
SELECT tupleMinus(vectorSum(tupleMultiply(tuple(1, 2), tuple(3, 4)), tuple(5, 6)), tuple(31, 41));

SELECT tupleDivide(tuple(5, 8, 11),tuple(-2, 2, 4));
SELECT tupleNegate(tuple(1, 0, 3.5));

SELECT dotProduct(tuple(1, 2, 3), tuple(2, 3, 4));
SELECT scalarProduct(tuple(-1, 2, 3.002), tuple(2, 3.4, 4));
