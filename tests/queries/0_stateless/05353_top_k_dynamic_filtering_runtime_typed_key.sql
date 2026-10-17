-- Top-K dynamic filtering must not drop rows when the key contains `Dynamic`, `Variant` or `JSON` below the
-- top level: the threshold is published as a `Field`, which does not keep their values' runtime type.

SET allow_suspicious_types_in_order_by = 1, allow_suspicious_types_in_group_by = 1;
SET use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 1;
-- The stateless test profile sets `max_rows_to_group_by`, which disables the GROUP BY top-K optimization.
SET enable_group_by_top_k_optimization = 1, enable_group_by_top_k_dynamic_filtering = 1, max_rows_to_group_by = 0;
SET query_plan_max_limit_for_top_k_optimization = 100, max_threads = 1;

DROP TABLE IF EXISTS t_topk_runtime_typed;

CREATE TABLE t_topk_runtime_typed (id UInt64, tv Tuple(Variant(Date, UInt64)), td Tuple(Dynamic), ad Array(Dynamic), j JSON)
ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES t_topk_runtime_typed;

-- Each part holds every other key, so the answer needs rows from both parts whichever is read first.
-- Keys are in 1000..2999 so that a threshold rebuilt from a `Field` gets a type that sorts below the stored one
-- (`Date` < `UInt64`, `UInt16` < `UInt32`, `Int16` < `Int64`).
INSERT INTO t_topk_runtime_typed SELECT number, tuple(number + 1000), tuple(toUInt32(number + 1000)), [toUInt32(number + 1000)], toJSONString(map('a', number + 1000))
FROM numbers(2000) WHERE number % 2 = 0;
INSERT INTO t_topk_runtime_typed SELECT number, tuple(number + 1000), tuple(toUInt32(number + 1000)), [toUInt32(number + 1000)], toJSONString(map('a', number + 1000))
FROM numbers(2000) WHERE number % 2 = 1;

SELECT tv FROM t_topk_runtime_typed ORDER BY tv LIMIT 3;
SELECT td FROM t_topk_runtime_typed ORDER BY td LIMIT 3;
SELECT ad FROM t_topk_runtime_typed ORDER BY ad LIMIT 3;
SELECT j FROM t_topk_runtime_typed ORDER BY j LIMIT 3;
SELECT j, count() FROM t_topk_runtime_typed GROUP BY j ORDER BY j LIMIT 3;

DROP TABLE t_topk_runtime_typed;
