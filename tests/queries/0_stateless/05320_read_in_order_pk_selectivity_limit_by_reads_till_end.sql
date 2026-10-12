-- A `LIMIT BY` that must drain its input (`exact_rows_before_limit` with `LIMIT BY ... LIMIT`) does not
-- let a `LIMIT` stop a read-in-order scan early, so the primary-key selectivity guard must still fire
-- and fall back to the parallel read plus sort.

DROP TABLE IF EXISTS rio_pk_selectivity_limit_by;

CREATE TABLE rio_pk_selectivity_limit_by (path String, key UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES rio_pk_selectivity_limit_by;

INSERT INTO rio_pk_selectivity_limit_by SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(0, 25000);
INSERT INTO rio_pk_selectivity_limit_by SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(25000, 25000);
INSERT INTO rio_pk_selectivity_limit_by SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(50000, 25000);
INSERT INTO rio_pk_selectivity_limit_by SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(75000, 25000);

SET max_threads = 4, enable_parallel_replicas = 0, read_in_order_use_virtual_row = 1, optimize_read_in_order = 1;

SELECT 'LIMIT BY with exact_rows_before_limit does not let the read stop early';
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT path FROM rio_pk_selectivity_limit_by
    WHERE path LIKE '%file.log'
    ORDER BY path
    LIMIT 1 BY path
    LIMIT 10
    SETTINGS read_in_order_max_primary_key_ratio = 0.5, exact_rows_before_limit = 1
) WHERE explain LIKE '%PartialSortingTransform%';

SELECT 'the same query with the guard disabled keeps read-in-order';
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT path FROM rio_pk_selectivity_limit_by
    WHERE path LIKE '%file.log'
    ORDER BY path
    LIMIT 1 BY path
    LIMIT 10
    SETTINGS read_in_order_max_primary_key_ratio = 1., exact_rows_before_limit = 1
) WHERE explain LIKE '%PartialSortingTransform%';

SELECT 'result';
SELECT path FROM rio_pk_selectivity_limit_by
WHERE path LIKE '%file.log'
ORDER BY path
LIMIT 1 BY path
LIMIT 3
SETTINGS read_in_order_max_primary_key_ratio = 0.5, exact_rows_before_limit = 1;

DROP TABLE rio_pk_selectivity_limit_by;
