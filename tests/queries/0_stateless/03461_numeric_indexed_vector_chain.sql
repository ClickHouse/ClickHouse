WITH
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))) AS vec1
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(numericIndexedVectorPointwiseSubtract(vec1, 10), 3));

WITH
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [0, 1, 2]))) AS vec1,
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [1, 2, 3]))) AS vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(numericIndexedVectorPointwiseMultiply(vec1, vec2), 2)) AS res1;
