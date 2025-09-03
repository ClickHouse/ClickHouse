-- Test TieredDistributedMerge engine registration and basic validation
CREATE TABLE test_tiered_distributed_merge_no_args ( `id` UInt32, `name` String ) ENGINE = TieredDistributedMerge();  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TABLE test_tiered_distributed_merge_one_arg ( `id` UInt32, `name` String ) ENGINE = TieredDistributedMerge(1);  -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
CREATE TABLE test_tiered_distributed_merge_invalid_first_arg ( `id` UInt32, `name` String) ENGINE = TieredDistributedMerge('invalid_arg', 1); -- { serverError BAD_ARGUMENTS }
CREATE TABLE test_tiered_distributed_merge_invalid_first_arg ( `id` UInt32, `name` String) ENGINE = TieredDistributedMerge(sin(3), 1);         -- { serverError BAD_ARGUMENTS }
CREATE TABLE test_tiered_distributed_merge_invalid_first_arg ( `id` UInt32, `name` String) ENGINE = TieredDistributedMerge(url('http://google.com', 'RawBLOB'), 1); -- { serverError BAD_ARGUMENTS }
CREATE TABLE test_tiered_distributed_merge_invalid_first_arg ( `id` UInt32, `name` String) ENGINE = TieredDistributedMerge(urlCluster('test_cluster', 'http://example.com')); -- { serverError BAD_ARGUMENTS }
CREATE TABLE test_tiered_distributed_merge_invalid_second_arg (`id` UInt32, `name` String) ENGINE = TieredDistributedMerge(remote('test_cluster', 'db', 'table'), 'invalid_predicate');-- { serverError BAD_ARGUMENTS }
