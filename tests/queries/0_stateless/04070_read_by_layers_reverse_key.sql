-- Test that readByLayers uses the correct ReadType for tables with reverse sorting keys.
-- Before the fix, readByLayers always used ReadType::InOrder, which produced wrong order
-- for tables with allow_experimental_reverse_key = 1 and descending primary keys.

DROP TABLE IF EXISTS t_reverse;

CREATE TABLE t_reverse (x Int32)
ENGINE = MergeTree()
ORDER BY (x DESC)
SETTINGS index_granularity = 1, allow_experimental_reverse_key = 1;

INSERT INTO t_reverse SELECT number FROM numbers(10);

-- Force the readByLayers code path via shard join optimization
-- and enable read_in_order_use_virtual_row which was part of the flaky combination.
SELECT x
FROM t_reverse
JOIN t_reverse AS t2 USING (x)
ORDER BY x
SETTINGS
    query_plan_join_shard_by_pk_ranges = 1,
    max_threads = 2,
    query_plan_read_in_order_through_join = 1,
    read_in_order_use_virtual_row = 1,
    enable_join_runtime_filters = 0;

DROP TABLE t_reverse;
