-- A `LEFT JOIN` preserves the output `LIMIT` as a bound on the left read, but a filter above the
-- join on the right-side columns can discard an arbitrary prefix of the joined rows. Then the
-- `LIMIT` no longer bounds the read and the primary-key selectivity guard must still reject
-- read-in-order on a poorly selective primary key.

DROP TABLE IF EXISTS rio_pk_selectivity_left;
DROP TABLE IF EXISTS rio_pk_selectivity_right;

CREATE TABLE rio_pk_selectivity_left (path String, key UInt64)
ENGINE = MergeTree ORDER BY path
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES rio_pk_selectivity_left;

INSERT INTO rio_pk_selectivity_left SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(0, 25000);
INSERT INTO rio_pk_selectivity_left SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(25000, 25000);
INSERT INTO rio_pk_selectivity_left SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(50000, 25000);
INSERT INTO rio_pk_selectivity_left SELECT concat('path/', toString(number % 1000), '/file.log'), number FROM numbers(75000, 25000);

CREATE TABLE rio_pk_selectivity_right (key UInt64) ENGINE = Memory;
INSERT INTO rio_pk_selectivity_right SELECT number FROM numbers(100000);

-- The filter on `r.key` stays above the join (the `IS NULL` branch keeps the outer join from being
-- converted to an inner one), so the guard fires and the plan sorts in parallel.
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT l.path
    FROM rio_pk_selectivity_left AS l
    LEFT JOIN rio_pk_selectivity_right AS r ON l.key = r.key
    WHERE l.path LIKE '%file.log' AND (r.key % 2 = 0 OR r.key IS NULL)
    ORDER BY l.path
    LIMIT 10
    SETTINGS max_threads = 4, enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 0.5,
        query_plan_read_in_order_through_join = 1, read_in_order_use_virtual_row = 1,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
        query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0,
        optimize_read_in_order = 1, join_algorithm = 'hash', join_use_nulls = 1
) WHERE explain LIKE '%PartialSortingTransform%';

-- Control: without the right-side filter the `LIMIT` still bounds the left read and read-in-order is kept.
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT l.path
    FROM rio_pk_selectivity_left AS l
    LEFT JOIN rio_pk_selectivity_right AS r ON l.key = r.key
    WHERE l.path LIKE '%file.log'
    ORDER BY l.path
    LIMIT 10
    SETTINGS max_threads = 4, enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 0.5,
        query_plan_read_in_order_through_join = 1, read_in_order_use_virtual_row = 1,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
        query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0,
        optimize_read_in_order = 1, join_algorithm = 'hash', join_use_nulls = 1
) WHERE explain LIKE '%PartialSortingTransform%';

-- Control: with the guard disabled the filtered query keeps read-in-order through the join, so the
-- first assertion is about the guard and not about the filter dropping the in-order read on its own.
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT l.path
    FROM rio_pk_selectivity_left AS l
    LEFT JOIN rio_pk_selectivity_right AS r ON l.key = r.key
    WHERE l.path LIKE '%file.log' AND (r.key % 2 = 0 OR r.key IS NULL)
    ORDER BY l.path
    LIMIT 10
    SETTINGS max_threads = 4, enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 1.,
        query_plan_read_in_order_through_join = 1, read_in_order_use_virtual_row = 1,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
        query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0,
        optimize_read_in_order = 1, join_algorithm = 'hash', join_use_nulls = 1
) WHERE explain LIKE '%PartialSortingTransform%';

DROP TABLE rio_pk_selectivity_left;
DROP TABLE rio_pk_selectivity_right;
