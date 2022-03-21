SELECT tupleHammingDistance(tuple(1), tuple(1));
SELECT tupleHammingDistance((1, 3), (1, 2));

SELECT (1, 2) + tupleMultiply((3, 4), materialize((5, 1))) - (6, 3);
SELECT vectorDifference(tuplePlus((1, 2), (3, 4)), (5, 6));
SELECT tupleMinus(materialize(vectorSum(tupleMultiply(materialize((1, 2)), (3, 4)), (5, 6))), (31, 41));
SELECT tupleDivide((5, 8, 11), (-2, 2, 4));
SELECT tuple(1) + tuple(2);

SELECT tupleNegate((1, 0, 3.5));
SELECT -materialize((1, 2, 3));
SELECT -tuple(1);

SELECT tupleMultiplyByNumber((1, 2, 3), 0.5);
SELECT tupleDivideByNumber((1, 2.5, 3), materialize(0.5));
SELECT tupleMultiplyByNumber(tuple(1), 1);
SELECT tupleDivideByNumber(tuple(1), materialize(1));

SELECT materialize((1, 2.0, 3.1)) * 3;
SELECT 5.5 * (2, 4);
SELECT (1, 2) / 2;
SELECT 2 / (1, 1); -- { serverError 43 }

SELECT tuple(1, 2, 3) * tuple(2, 3, 4);
SELECT dotProduct(materialize((-1, 2, 3.002)), materialize((2, 3.4, 4)));
SELECT scalarProduct(tuple(1), tuple(0));

SELECT L1Norm((-1, 2, -3));
SELECT L1Norm((-1, 2.5, -3.6));
SELECT L2Norm((1, 1.0));
SELECT L2Norm(materialize((-12, 5)));

SELECT max2(materialize(1), 1.5);
SELECT min2(-1, -3);
SELECT LinfNorm((1, -2.3, 1.7));

SELECT LpNorm(tuple(-1), 3.3);
SELECT LpNorm(tuple(-1), 3);
SELECT LpNorm(tuple(-1.1), 3);
SELECT LpNorm((95800, 217519, 414560), 4);
SELECT LpNorm((13, -84.4, 91, 63.1), 2) = L2Norm(tuple(13, -84.4, 91, 63.1));
SELECT LpNorm(materialize((13, -84.4, 91, 63.1)), 1) = L1Norm(tuple(13, -84.4, 91, 63.1));
SELECT LpNorm((-1, -2), 11.);

SELECT L1Distance((1, 2, 3), (2, 3, 1));
SELECT L2Distance(materialize((1, 1)), (3, -1));
SELECT LinfDistance((1, 1), (1, 2));
SELECT L2Distance((5, 5), (5, 5));
SELECT LpDistance((1800, 1900), (18, 59), 12) - LpDistance(tuple(-22), tuple(1900), 12.);

SELECT L1Normalize(materialize((1, -4)));
SELECT L2Normalize((3, 4));
SELECT LinfNormalize((5, -5, 5.0));
SELECT LpNormalize((1, pow(31, 1 / 5)), 5.);

SELECT cosineDistance(materialize((1, 1)), (2, 2));
SELECT cosineDistance((1, 1), materialize((-3, 3.0)));
SELECT cosineDistance((1, 1), (-1, -1));
SELECT cosineDistance((1, 0), (0.5, sqrt(3) / 2));

SELECT (NULL, 1) + (1, NULL);
SELECT (NULL, 1) * materialize((1, NULL));
SELECT L2Norm((NULL, 3, 4));
SELECT 2 * (1, 2, NULL);
SELECT (1, 1.0, NULL) / NULL;
SELECT (1, 1.0, NULL) / materialize(NULL);
SELECT -(NULL, NULL, 1);
SELECT (NULL, NULL) * NULL;
SELECT L1Normalize((NULL, 1));
SELECT cosineDistance((NULL, 1), (NULL, NULL));
SELECT max2(NULL, 1) - min2(NULL, 1);

SELECT L1Norm(1); -- { serverError 43 }
SELECT (1, 1) / toString(1); -- { serverError 43 }
SELECT -(1, toString(1)); -- { serverError 43 }
SELECT LpNorm((1, 2), toDecimal32(2, 4)); -- { serverError 43 }
SELECT (1, 2) * toDecimal32(3.1, 8);

SELECT cosineDistance((1, 2), (2, 3, 4)); -- { serverError 43 }
SELECT tuple() + tuple(); -- { serverError 42 }
SELECT LpNorm((1, 2, 3)); -- { serverError 42 }
SELECT max2(1, 2, -1); -- { serverError 42 }

SELECT LpNorm((1, 2, 3), materialize(4.)); -- { serverError 44 }

SELECT tuple(*, 1) + tuple(2, *) FROM numbers(3);
SELECT LpDistance(tuple(*, 1), tuple(2, *), * + 1.) FROM numbers(3, 2); -- { serverError 44 }
SELECT cosineDistance(tuple(*, * + 1), tuple(1, 2)) FROM numbers(1, 3);
SELECT -tuple(NULL, * * 2, *) FROM numbers(2);

SELECT normL1((1, 1)), normL2((1, 1)), normLinf((1, 1)), normLp((1, 1), 1.);
SELECT distanceL1((1, 1), (1, 1)), distanceL2((1, 1), (1, 1)), distanceLinf((1, 1), (1, 1)), distanceLp((1, 1), (1, 1), 1.);
SELECT normalizeL1((1, 1)), normalizeL2((1, 1)), normalizeLinf((1, 1)), normalizeLp((1, 1), 1.);

SELECT LpNorm((1, 2, 3), 2.2);
SELECT LpNorm((1.5, 2.5, 4), pi());
SELECT LpNorm((3, 1, 4), 0); -- { serverError 69 }
SELECT LpNorm((1, 2, 3), 0.5); -- { serverError 69 }
SELECT LpNorm((1, 2, 3), inf); -- { serverError 69 }
SELECT LpNorm((1, 2, 3), -1.); -- { serverError 69 }
SELECT LpNorm((1, 2, 3), -1); -- { serverError 44 }
SELECT LpNorm((1, 2, 3), 0.); -- { serverError 69 }
SELECT cosineDistance(materialize((NULL, -2147483648)), (1048577, 1048575));

-- not extra parentheses
EXPLAIN SYNTAX SELECT -((3, 7, 3), 100);
