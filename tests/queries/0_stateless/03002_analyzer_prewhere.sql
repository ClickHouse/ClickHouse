SET max_threads = 16, receive_timeout = 10., receive_data_timeout_ms = 10000, allow_suspicious_low_cardinality_types = true, enable_positional_arguments = false, log_queries = true, table_function_remote_max_addresses = 200, any_join_distinct_right_table_keys = true, joined_subquery_requires_alias = false, enable_analyzer = true, max_execution_time = 10., max_memory_usage = 10000000000, log_comment = '/workspace/ch/tests/queries/0_stateless/01710_projection_in_index.sql', send_logs_level = 'fatal', enable_optimize_predicate_expression = false, prefer_localhost_replica = true, allow_introspection_functions = true, optimize_functions_to_subcolumns = false, transform_null_in = true, optimize_use_projections = true, allow_deprecated_syntax_for_merge_tree = true, parallelize_output_from_storages = false;

CREATE TABLE t__fuzz_0 (`i` Int32, `j` Nullable(Int32), `k` Int32, PROJECTION p (SELECT * ORDER BY j)) ENGINE = MergeTree ORDER BY i SETTINGS index_granularity = 1, allow_nullable_key=1;

INSERT INTO t__fuzz_0 SELECT * FROM generateRandom() LIMIT 3;
INSERT INTO t__fuzz_0 SELECT * FROM generateRandom() LIMIT 3;
INSERT INTO t__fuzz_0 SELECT * FROM generateRandom() LIMIT 3;
INSERT INTO t__fuzz_0 SELECT * FROM generateRandom() LIMIT 3;
INSERT INTO t__fuzz_0 SELECT * FROM generateRandom() LIMIT 3;

SELECT * FROM t__fuzz_0 PREWHERE (i < 5) AND (j IN (1, 2)) WHERE i < 5;
DROP TABLE t__fuzz_0;
