-- Tags: replica, distributed

drop table if exists test_max_parallel_replicas_lr;

-- If you wonder why the table is named with "_lr" suffix in this test.
-- No reason. Actually it is the name of the table in our customer and they provided this test case for us.

CREATE TABLE test_max_parallel_replicas_lr (timestamp UInt64) ENGINE = MergeTree ORDER BY (intHash32(timestamp)) SAMPLE BY intHash32(timestamp);
INSERT INTO test_max_parallel_replicas_lr select number as timestamp from system.numbers limit 100;

SET enable_parallel_replicas = 1;
SET parallel_replicas_mode='sampling_key';
SET max_parallel_replicas = 2;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
select count() FROM remote('127.0.0.{2|3}', currentDatabase(), test_max_parallel_replicas_lr) PREWHERE timestamp > 0;

drop table test_max_parallel_replicas_lr;
