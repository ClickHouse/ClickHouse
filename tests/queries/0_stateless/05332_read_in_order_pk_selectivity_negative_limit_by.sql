-- A negative `LIMIT BY` drains its input: it cannot know the last rows of any group before the end of
-- the input. A `LIMIT` above it therefore does not let a read-in-order scan stop early, so the
-- primary-key selectivity guard must still fire and fall back to the parallel read plus sort.

DROP TABLE IF EXISTS rio_pk_selectivity_negative_limit_by;

CREATE TABLE rio_pk_selectivity_negative_limit_by (path String, g UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES rio_pk_selectivity_negative_limit_by;

INSERT INTO rio_pk_selectivity_negative_limit_by SELECT concat('path/', toString(number % 1000), '/file.log'), number % 7 FROM numbers(0, 25000);
INSERT INTO rio_pk_selectivity_negative_limit_by SELECT concat('path/', toString(number % 1000), '/file.log'), number % 7 FROM numbers(25000, 25000);
INSERT INTO rio_pk_selectivity_negative_limit_by SELECT concat('path/', toString(number % 1000), '/file.log'), number % 7 FROM numbers(50000, 25000);
INSERT INTO rio_pk_selectivity_negative_limit_by SELECT concat('path/', toString(number % 1000), '/file.log'), number % 7 FROM numbers(75000, 25000);

SET max_threads = 4, enable_parallel_replicas = 0, read_in_order_use_virtual_row = 1, optimize_read_in_order = 1;

SELECT 'negative LIMIT BY does not let the read stop early';
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT path FROM rio_pk_selectivity_negative_limit_by
    WHERE path LIKE '%file.log'
    ORDER BY path
    LIMIT -1 BY g
    LIMIT 10
    SETTINGS read_in_order_max_primary_key_ratio = 0.5
) WHERE explain LIKE '%PartialSortingTransform%';

SELECT 'the same query with the guard disabled keeps read-in-order';
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT path FROM rio_pk_selectivity_negative_limit_by
    WHERE path LIKE '%file.log'
    ORDER BY path
    LIMIT -1 BY g
    LIMIT 10
    SETTINGS read_in_order_max_primary_key_ratio = 1.
) WHERE explain LIKE '%PartialSortingTransform%';

SELECT 'result';
SELECT path, g FROM rio_pk_selectivity_negative_limit_by
WHERE path LIKE '%file.log'
ORDER BY path, g
LIMIT -1 BY g
LIMIT 3
SETTINGS read_in_order_max_primary_key_ratio = 0.5;

DROP TABLE rio_pk_selectivity_negative_limit_by;
