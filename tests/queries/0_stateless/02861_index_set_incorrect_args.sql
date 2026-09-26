-- https://github.com/ClickHouse/ClickHouse/issues/52019
DROP TABLE IF EXISTS set_index__fuzz_41;
CREATE TABLE set_index__fuzz_41 (`a` Date, `b` Nullable(DateTime64(3)), INDEX b_set b TYPE set(0) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO set_index__fuzz_41 (a) VALUES (today());
SELECT b FROM set_index__fuzz_41 WHERE and(b = 256) SETTINGS force_data_skipping_indices = 'b_set', optimize_move_to_prewhere = 0, max_parallel_replicas=2, parallel_replicas_for_non_replicated_merge_tree=1, enable_parallel_replicas=2; -- { serverError TOO_FEW_ARGUMENTS_FOR_FUNCTION }
DROP TABLE set_index__fuzz_41;
