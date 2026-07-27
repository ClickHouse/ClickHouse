-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

DROP TABLE IF EXISTS t_read_rejects;
CREATE TABLE t_read_rejects (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_read_rejects SELECT number FROM numbers(200000);

-- Distributed aggregation cannot enforce a global max_rows_to_group_by, so pin it to 0 (randomized
-- settings set it nonzero, which would make make_distributed_plan reject the count/sum below).
SET max_rows_to_group_by = 0;

SET distributed_plan_default_shuffle_join_bucket_count = 3, distributed_plan_default_reader_bucket_count = 3;
SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0;

-- A distributed read cannot reproduce the coordinator's part ordering, so the part-order virtual
-- columns are rejected at planning time (rather than silently returning worker-local values).
SELECT _part_index FROM t_read_rejects; -- { serverError SUPPORT_IS_DISABLED }
SELECT _part_starting_offset FROM t_read_rejects; -- { serverError SUPPORT_IS_DISABLED }

-- Bucket counts size the exchange fan-out, so they are capped to limit memory consumption: an
-- oversized value is rejected at planning time instead of allocating that many tasks and ports.
SELECT sum(x) FROM t_read_rejects SETTINGS distributed_plan_default_shuffle_join_bucket_count = 257; -- { serverError INVALID_SETTING_VALUE }
SELECT sum(x) FROM t_read_rejects SETTINGS distributed_plan_default_reader_bucket_count = 257; -- { serverError INVALID_SETTING_VALUE }
-- The validation runs before the tryMakeDistributedRead pass sizes any vector, so a value near the type
-- maximum is rejected too instead of sizing a read-bucket vector to it and aborting (std::length_error).
SELECT sum(x) FROM t_read_rejects SETTINGS distributed_plan_default_shuffle_join_bucket_count = 9223372036854775807; -- { serverError INVALID_SETTING_VALUE }
SELECT sum(x) FROM t_read_rejects SETTINGS distributed_plan_default_reader_bucket_count = 9223372036854775807; -- { serverError INVALID_SETTING_VALUE }

-- _part_offset alone is per-part and order-independent, so it stays supported.
SELECT sum(_part_offset) FROM t_read_rejects;

-- A per-block function must keep its global numbering: the GatherExchange is not pushed below it,
-- so the row numbers stay a single 0..N-1 sequence (sum is order-independent).
SELECT sum(rn) FROM (SELECT rowNumberInAllBlocks() AS rn FROM t_read_rejects);

DROP TABLE t_read_rejects;
