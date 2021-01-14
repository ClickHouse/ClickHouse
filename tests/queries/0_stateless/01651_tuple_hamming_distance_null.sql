SELECT tupleHammingDistance((1, 2), (2, 3));

SELECT tupleHammingDistance(1, 2); --{ serverError 43 }

SELECT tupleHammingDistance(1, (2, 3)); --{ serverError 43 }

SELECT tupleHammingDistance((1, 2, 3), (2, 3)); -- { serverError 43 }

SELECT tupleHammingDistance((1, NULL), (2, 3)); -- { serverError 43 }

SELECT tupleHammingDistance((1, 2), (2, NULL)); -- { serverError 43 }
