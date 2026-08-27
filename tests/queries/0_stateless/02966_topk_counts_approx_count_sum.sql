WITH
    arraySlice(arrayReverseSort(x -> (x.2, x.1), arrayZip(untuple(sumMap(([k], [1]))))), 1, 5) AS topKExact,
    arraySlice(arrayReverseSort(x -> (x.2, x.1), arrayZip(untuple(sumMap(([k], [w]))))), 1, 5) AS topKWeightedExact
SELECT
    topKExact,
    topKWeightedExact,
    topK(3, 2, 'counts')(k) AS topK_counts,
    topKWeighted(3, 2, 'counts')(k, w) AS topKWeighted_counts,
    approx_top_count(3, 6)(k) AS approx_top_count,
    approx_top_k(3, 6)(k) AS approx_top_k,
    approx_top_sum(3, 6)(k, w) AS approx_top_sum
FROM
(
    SELECT
        concat(countDigits(number * number), '_', intDiv((number % 10), 7)) AS k,
        number AS w
    FROM numbers(1000)
);

SELECT topKMerge(4, 2, 'counts')(state) FROM ( SELECT topKState(4, 2, 'counts')(countDigits(number * number)) AS state FROM numbers(1000));

SELECT topKMerge(4, 3, 'counts')(state) FROM ( SELECT topKState(4, 2, 'counts')(countDigits(number * number)) AS state FROM numbers(1000)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT topKMerge(4, 2)(state) FROM ( SELECT topKState(4, 2, 'counts')(countDigits(number * number)) AS state FROM numbers(1000)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT topKMerge(state) FROM ( SELECT topKState(4, 2, 'counts')(countDigits(number * number)) AS state FROM numbers(1000)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }