CREATE TABLE tabc__fuzz_21 (`a` Int64, `b` Int8 ALIAS a + 1, `c` DateTime ALIAS b + 1, `s` Nullable(DateTime64(3))) ENGINE = MergeTree ORDER BY a;
INSERT INTO tabc__fuzz_21 (a, s) SELECT number, concat('abc', toString(number)) FROM numbers(4);

CREATE TABLE tb__fuzz_0 (`b` Nullable(Int32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tb__fuzz_0 SELECT number FROM numbers(4);


SELECT (*, 8, 8, 8, toFixedString('FirstKey', 8), 8, 8, (toNullable(8) IS NOT NULL) IS NULL, 8, toNullable(toNullable(toNullable(8))), 8, 8, 'FirstKey', (8 IS NOT NULL) IS NOT NULL), *, b + 1 AS a FROM tb__fuzz_0 GLOBAL ALL INNER JOIN tabc__fuzz_21 USING (a) ORDER BY `ALL` DESC SETTINGS enable_analyzer = 1, max_block_size = 900, max_threads = 20, receive_timeout = 10., receive_data_timeout_ms = 10000, allow_suspicious_low_cardinality_types = true, merge_tree_min_rows_for_concurrent_read = 1000, log_queries = true, table_function_remote_max_addresses = 200, join_use_nulls = true, max_execution_time = 10., read_in_order_use_virtual_row = true, allow_introspection_functions = true, mutations_sync = 2, optimize_trivial_insert_select = true, aggregate_functions_null_for_empty = true, enable_filesystem_cache = false, allow_prefetched_read_pool_for_remote_filesystem = false, parallel_replicas_for_cluster_engines = false, allow_experimental_analyzer = true, analyzer_compatibility_join_using_top_level_identifier = true;
