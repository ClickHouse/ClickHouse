-- A SQL `LIMIT` exempts a query from the primary-key selectivity guard, because read-in-order can
-- finish early and skip whole parts. That only holds while the `LIMIT` is allowed to stop its input:
-- with `exact_rows_before_limit` (and in the `WITH TOTALS` cases behind `limitAlwaysReadsTillEnd`)
-- the `LimitStep` is built with `always_read_till_end` and the whole stream is consumed anyway, so
-- the numeric bound does not shorten the read and a poorly-selective in-order read serializes a
-- full scan. The guard must fire there.

DROP TABLE IF EXISTS rio_pk_selectivity_till_end;

CREATE TABLE rio_pk_selectivity_till_end (path String, key UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES rio_pk_selectivity_till_end;

INSERT INTO rio_pk_selectivity_till_end SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(0, 25000);
INSERT INTO rio_pk_selectivity_till_end SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(25000, 25000);
INSERT INTO rio_pk_selectivity_till_end SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(50000, 25000);
INSERT INTO rio_pk_selectivity_till_end SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(75000, 25000);

-- `optimize_read_in_order` and the other read-in-order switches are randomized by the test runner;
-- the assertions below are about the guard, so pin them. `max_threads` is above `1` because the
-- guard only fires when the parallel read has more than one stream to recover.
SET max_threads = 4, enable_parallel_replicas = 0, read_in_order_use_virtual_row = 1,
    optimize_read_in_order = 1;

SELECT 'plain LIMIT keeps read-in-order';
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT path FROM rio_pk_selectivity_till_end
    WHERE path LIKE '%file.log'
    ORDER BY path
    LIMIT 10
    SETTINGS read_in_order_max_primary_key_ratio = 0.5
) WHERE explain LIKE '%PartialSortingTransform%';

SELECT 'exact_rows_before_limit does not let the read stop early';
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT path FROM rio_pk_selectivity_till_end
    WHERE path LIKE '%file.log'
    ORDER BY path
    LIMIT 10
    SETTINGS read_in_order_max_primary_key_ratio = 0.5, exact_rows_before_limit = 1
) WHERE explain LIKE '%PartialSortingTransform%';

-- Control: the same query with the guard disabled keeps read-in-order, so the assertion above is
-- about the guard and not about `exact_rows_before_limit` changing the plan by itself.
SELECT 'exact_rows_before_limit with the guard disabled keeps read-in-order';
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT path FROM rio_pk_selectivity_till_end
    WHERE path LIKE '%file.log'
    ORDER BY path
    LIMIT 10
    SETTINGS read_in_order_max_primary_key_ratio = 1., exact_rows_before_limit = 1
) WHERE explain LIKE '%PartialSortingTransform%';

-- The fallback must keep answering the query correctly, including the exact count of rows before
-- the `LIMIT` that `exact_rows_before_limit` promises.
SELECT 'result';
SELECT path FROM rio_pk_selectivity_till_end
WHERE path LIKE '%file.log'
ORDER BY path
LIMIT 3
SETTINGS read_in_order_max_primary_key_ratio = 0.5, exact_rows_before_limit = 1;

DROP TABLE rio_pk_selectivity_till_end;
