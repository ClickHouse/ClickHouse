DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY (c0 DESC) SETTINGS index_granularity = 1, allow_experimental_reverse_key = 1;

INSERT INTO TABLE t0 (c0) SELECT number FROM numbers(10);

SELECT c0 FROM t0 JOIN t0 tx USING (c0) ORDER BY c0 SETTINGS query_plan_join_shard_by_pk_ranges = 1, max_threads = 2;

SELECT '---';

-- Same query with three settings pinned to trigger the read-in-order path through join sharding:
-- query_plan_read_in_order_through_join=1 lets optimizeReadInOrder traverse through the JOIN,
-- read_in_order_use_virtual_row=1 enables read-in-order for INNER JOIN,
-- enable_join_runtime_filters=0 lets optimizeJoinByShards see the right-side source.
SELECT c0 FROM t0 JOIN t0 tx USING (c0) ORDER BY c0 SETTINGS query_plan_join_shard_by_pk_ranges = 1, max_threads = 2, query_plan_read_in_order_through_join = 1, read_in_order_use_virtual_row = 1, enable_join_runtime_filters = 0;

DROP TABLE t0;
