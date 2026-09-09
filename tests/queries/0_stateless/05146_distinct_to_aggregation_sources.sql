SET query_plan_convert_distinct_to_aggregation = 1;
SET query_plan_remove_redundant_distinct = 0;
SET max_threads = 2;

-- Sources without an explicit bound retain streaming deduplication.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 3 FROM system.numbers);
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT prime % 3 FROM system.primes);
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number FROM loop(numbers(10)));

-- Arguments that bound the source allow aggregation.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 3 FROM numbers(10));
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT prime % 3 FROM primes(10));

-- A limit below `DISTINCT` bounds the input before aggregation consumes it.
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT number % 3 FROM (SELECT number FROM system.numbers LIMIT 10));
SELECT countIf(explain LIKE '%AggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT prime % 3 FROM (SELECT prime FROM system.primes LIMIT 10));

-- Bounded prime sources preserve their distinct keys and empty results.
SELECT arraySort(groupArray(prime)) FROM (SELECT DISTINCT prime FROM primes(10));
SELECT count() FROM (SELECT DISTINCT prime FROM primes(0));
