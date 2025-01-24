SET merge_table_max_tables_to_look_for_schema_inference = 1;

CREATE TABLE test_s64_local (date Date, value Int64) ENGINE = MergeTree order by tuple();
CREATE TABLE test_u64_local (date Date, value UInt64) ENGINE = MergeTree order by tuple();
CREATE TABLE test_s64_distributed AS test_s64_local ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_s64_local, rand());
CREATE TABLE test_u64_distributed AS test_u64_local ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_u64_local, rand());

SELECT * FROM merge(currentDatabase(), '') WHERE value = 1048575;
