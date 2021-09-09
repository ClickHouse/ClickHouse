-- { echo }
SELECT uniqCombinedIfMerge(n) FROM (SELECT uniqCombinedIfState(number, number % 2) AS n, max(number) AS last FROM numbers(10));
SELECT uniqCombinedIfMergeIf(n, last > 50) FROM (SELECT uniqCombinedIfState(number, number % 2) AS n, max(number) AS last FROM numbers(10));
SELECT uniqCombinedIfMergeIf(n, last > 50) FILTER(WHERE last>50) FROM (SELECT uniqCombinedIfState(number, number % 2) AS n, max(number) AS last FROM numbers(10));
SELECT uniqCombinedIfMergeIf(n, last > 5) FROM (SELECT uniqCombinedIfState(number, number % 2) AS n, max(number) AS last FROM numbers(10));
SELECT uniqCombinedIfMergeIfIf(n, last > 5) FROM (SELECT uniqCombinedIfState(number, number % 2) AS n, max(number) AS last FROM numbers(10)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT uniqCombinedIfMergeIfIf(n, last > 5, 1) FROM (SELECT uniqCombinedIfState(number, number % 2) AS n, max(number) AS last FROM numbers(10)); -- { serverError ILLEGAL_AGGREGATION }
