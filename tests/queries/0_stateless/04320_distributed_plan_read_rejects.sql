-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

DROP TABLE IF EXISTS t_read_rejects;
CREATE TABLE t_read_rejects (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_read_rejects SELECT number FROM numbers(200000);

DROP TABLE IF EXISTS t_read_rejects_final;
CREATE TABLE t_read_rejects_final (k UInt64, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY k;
INSERT INTO t_read_rejects_final SELECT number, 1 FROM numbers(100000);
INSERT INTO t_read_rejects_final SELECT number, 2 FROM numbers(100000);

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

-- The Cascades node count sizes the read buckets and the exchange fan-out the same way, so it is
-- capped identically, wherever it came from: the setting, the internal parameter, or a configured
-- worker cluster. A count above the cap could not produce a distributed read anyway, since the read
-- would split into that many buckets and then be refused for exceeding the same cap.
SELECT count() FROM t_read_rejects SETTINGS enable_cascades_optimizer = 1, distributed_plan_workers_num = 257; -- { serverError INVALID_SETTING_VALUE }
SELECT count() FROM t_read_rejects SETTINGS enable_cascades_optimizer = 1, distributed_plan_workers_num = 9223372036854775807; -- { serverError INVALID_SETTING_VALUE }
SET param__internal_cascades_cluster_node_count = 9223372036854775807;
SELECT count() FROM t_read_rejects SETTINGS enable_cascades_optimizer = 1, distributed_plan_workers_num = 4; -- { serverError INVALID_SETTING_VALUE }
SET param__internal_cascades_cluster_node_count = 0;
-- FINAL derives a larger layer budget from the count before slicing, so it is capped by the same check.
SELECT sum(v) FROM t_read_rejects_final FINAL SETTINGS enable_cascades_optimizer = 1, distributed_plan_workers_num = 9223372036854775807; -- { serverError INVALID_SETTING_VALUE }
-- The cap is inclusive: 256 is accepted, on both read paths. An over-strict cap raises here rather
-- than falling back to a serial read, so these answering at all is the boundary assertion.
SELECT count() FROM t_read_rejects SETTINGS enable_cascades_optimizer = 1, distributed_plan_workers_num = 256;
SELECT sum(v) FROM t_read_rejects_final FINAL SETTINGS enable_cascades_optimizer = 1, distributed_plan_workers_num = 256;
-- Without Cascades the count never becomes a bucket count, so it is not capped: the rule-based
-- distributed plan takes its bucket counts from the settings validated above.
SELECT count() FROM t_read_rejects SETTINGS enable_cascades_optimizer = 0, distributed_plan_workers_num = 9223372036854775807;

-- _part_offset alone is per-part and order-independent, so it stays supported.
SELECT sum(_part_offset) FROM t_read_rejects;

-- A per-block function must keep its global numbering: the GatherExchange is not pushed below it,
-- so the row numbers stay a single 0..N-1 sequence (sum is order-independent).
SELECT sum(rn) FROM (SELECT rowNumberInAllBlocks() AS rn FROM t_read_rejects);

DROP TABLE t_read_rejects;
DROP TABLE t_read_rejects_final;
