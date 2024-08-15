SET allow_experimental_analyzer = 1;

EXPLAIN QUERY TREE SELECT uniqCombined(tuple('')) FROM numbers(1);

SELECT uniqCombined(tuple('')) FROM numbers(1);
