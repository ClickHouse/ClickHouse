-- The SQL `LIMIT` exempts a query from the primary-key selectivity guard because read-in-order
-- can finish early. That only holds while every step between the read and the `LIMIT` preserves
-- the limit: a preliminary `DISTINCT`, an `arrayJoin` expression, or a non-LEFT `ARRAY JOIN`
-- can consume an arbitrary prefix of the input, so the guard must still fire for those shapes.

DROP TABLE IF EXISTS rio_pk_selectivity_limit;

CREATE TABLE rio_pk_selectivity_limit (path String, key UInt64, arr Array(UInt8))
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES rio_pk_selectivity_limit;

INSERT INTO rio_pk_selectivity_limit SELECT concat('path/', toString(number % 1000), '/file.log'), number, [1] FROM numbers(0, 25000);
INSERT INTO rio_pk_selectivity_limit SELECT concat('path/', toString(number % 1000), '/file.log'), number, [1] FROM numbers(25000, 25000);
INSERT INTO rio_pk_selectivity_limit SELECT concat('path/', toString(number % 1000), '/file.log'), number, [1] FROM numbers(50000, 25000);
INSERT INTO rio_pk_selectivity_limit SELECT concat('path/', toString(number % 1000), '/file.log'), number, [1] FROM numbers(75000, 25000);

-- `optimize_read_in_order` is randomized by the test runner; the assertions below are about the
-- guard, so pin it (and the other read-in-order switches) to keep the plan under test.
SET max_threads = 4, enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 0.5, read_in_order_use_virtual_row = 1,
    optimize_read_in_order = 1;

SELECT 'plain LIMIT keeps read-in-order';
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT path FROM rio_pk_selectivity_limit
    WHERE path LIKE '%file.log'
    ORDER BY path
    LIMIT 10
) WHERE explain LIKE '%PartialSortingTransform%';

SELECT 'preliminary DISTINCT does not preserve the LIMIT';
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT DISTINCT path FROM rio_pk_selectivity_limit
    WHERE path LIKE '%file.log'
    ORDER BY path
    LIMIT 10
    SETTINGS optimize_distinct_in_order = 0
) WHERE explain LIKE '%PartialSortingTransform%';

SELECT 'arrayJoin does not preserve the LIMIT';
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT arrayJoin(arr) FROM rio_pk_selectivity_limit
    WHERE path LIKE '%file.log'
    ORDER BY path
    LIMIT 10
) WHERE explain LIKE '%PartialSortingTransform%';

SELECT 'non-LEFT ARRAY JOIN does not preserve the LIMIT';
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT path, a FROM rio_pk_selectivity_limit
    ARRAY JOIN arr AS a
    WHERE path LIKE '%file.log'
    ORDER BY path
    LIMIT 10
) WHERE explain LIKE '%PartialSortingTransform%';

SELECT 'LEFT ARRAY JOIN preserves the LIMIT and keeps read-in-order';
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT path, a FROM rio_pk_selectivity_limit
    LEFT ARRAY JOIN arr AS a
    WHERE path LIKE '%file.log'
    ORDER BY path
    LIMIT 10
) WHERE explain LIKE '%PartialSortingTransform%';

DROP TABLE rio_pk_selectivity_limit;
