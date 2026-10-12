-- A plain `LIMIT BY` below the `LIMIT` and `LIMIT ... WITH TIES` do not bound a read-in-order scan:
-- `LIMIT BY` may discard a long prefix of the sorted stream, and the tie group of the last row is
-- unbounded. So the primary-key selectivity guard must still fire and fall back to the parallel
-- read plus sort, while a plain `LIMIT` keeps read-in-order.

DROP TABLE IF EXISTS rio_pk_selectivity_limit_by_ties;

CREATE TABLE rio_pk_selectivity_limit_by_ties (path String, key UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES rio_pk_selectivity_limit_by_ties;

INSERT INTO rio_pk_selectivity_limit_by_ties SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(0, 25000);
INSERT INTO rio_pk_selectivity_limit_by_ties SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(25000, 25000);
INSERT INTO rio_pk_selectivity_limit_by_ties SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(50000, 25000);
INSERT INTO rio_pk_selectivity_limit_by_ties SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(75000, 25000);

SET max_threads = 4, enable_parallel_replicas = 0, read_in_order_use_virtual_row = 1, optimize_read_in_order = 1, exact_rows_before_limit = 0;

SELECT 'plain LIMIT keeps read-in-order';
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT path FROM rio_pk_selectivity_limit_by_ties
    WHERE path LIKE '%file.log'
    ORDER BY path
    LIMIT 10
    SETTINGS read_in_order_max_primary_key_ratio = 0.5
) WHERE explain LIKE '%PartialSortingTransform%';

SELECT 'LIMIT BY does not let the read stop early';
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT path FROM rio_pk_selectivity_limit_by_ties
    WHERE path LIKE '%file.log'
    ORDER BY path
    LIMIT 1 BY path
    LIMIT 10
    SETTINGS read_in_order_max_primary_key_ratio = 0.5
) WHERE explain LIKE '%PartialSortingTransform%';

SELECT 'LIMIT WITH TIES does not let the read stop early';
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT path FROM rio_pk_selectivity_limit_by_ties
    WHERE path LIKE '%file.log'
    ORDER BY path
    LIMIT 10 WITH TIES
    SETTINGS read_in_order_max_primary_key_ratio = 0.5
) WHERE explain LIKE '%PartialSortingTransform%';

SELECT 'results';
SELECT path FROM rio_pk_selectivity_limit_by_ties
WHERE path LIKE '%file.log'
ORDER BY path
LIMIT 1 BY path
LIMIT 3
SETTINGS read_in_order_max_primary_key_ratio = 0.5;

SELECT count(), uniqExact(path) FROM
(
    SELECT path FROM rio_pk_selectivity_limit_by_ties
    WHERE path LIKE '%file.log'
    ORDER BY path
    LIMIT 10 WITH TIES
    SETTINGS read_in_order_max_primary_key_ratio = 0.5
);

DROP TABLE rio_pk_selectivity_limit_by_ties;
