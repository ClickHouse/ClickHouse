SET enable_analyzer = 1;
SET max_threads=4;

DROP TABLE IF EXISTS trivial_count;
CREATE TABLE trivial_count ORDER BY number AS Select * from numbers(10);

-- { echo On }
-- We should use just a single thread to merge the state of trivial count
EXPLAIN PIPELINE SELECT count() FROM trivial_count;

-- But not if we are filtering or doing other operations (no trivial count)
EXPLAIN PIPELINE SELECT count() FROM trivial_count WHERE number % 3 = 2;
EXPLAIN PIPELINE SELECT count() FROM trivial_count GROUP BY number % 10;

-- Other aggregations should still use as many threads as necessary
EXPLAIN PIPELINE SELECT sum(number) FROM trivial_count;
EXPLAIN PIPELINE SELECT count(), sum(number) FROM trivial_count;