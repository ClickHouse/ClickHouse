-- The output LIMIT cannot bound the left read through an INNER JOIN, because the join can
-- discard left rows. The primary-key selectivity guard must therefore still reject read-in-order.

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

SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT l.path
    FROM rio_pk_selectivity_left AS l
    INNER JOIN rio_pk_selectivity_right AS r ON l.key = r.key
    WHERE l.path LIKE '%file.log'
    ORDER BY l.path
    LIMIT 10
    SETTINGS max_threads = 4, enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 0.5,
        query_plan_read_in_order_through_join = 1, read_in_order_use_virtual_row = 1,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
        query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0
) WHERE explain LIKE '%PartialSortingTransform%';

-- Control: with the guard disabled the same plan does keep read-in-order through the join, so the
-- assertion above is about the guard and not about the join dropping the in-order read on its own.
-- A spilling hash join does not preserve the left side order, hence the two `max_bytes_*` settings,
-- and the join order is pinned so that the `MergeTree` table stays on the left (probe) side.
SELECT count() > 0 FROM
(
    EXPLAIN PIPELINE
    SELECT l.path
    FROM rio_pk_selectivity_left AS l
    INNER JOIN rio_pk_selectivity_right AS r ON l.key = r.key
    WHERE l.path LIKE '%file.log'
    ORDER BY l.path
    LIMIT 10
    SETTINGS max_threads = 4, enable_parallel_replicas = 0, read_in_order_max_primary_key_ratio = 1.,
        query_plan_read_in_order_through_join = 1, read_in_order_use_virtual_row = 1,
        max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
        query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0
) WHERE explain LIKE '%PartialSortingTransform%';

DROP TABLE rio_pk_selectivity_left;
DROP TABLE rio_pk_selectivity_right;
