-- Tags: no-random-settings, no-random-merge-tree-settings

create table t(s LowCardinality(String), s2 String) Engine = MergeTree order by tuple() settings index_granularity = 100;

insert into t select repeat('x', number%100) as s, s as s2 from numbers_mt(3e5);

set max_result_bytes = 0, max_result_rows = 0;
set max_threads = 32, max_block_size = 100, max_memory_usage = '1Gi', join_algorithm = 'parallel_hash';
select * from t t1 join t t2 on t1.s = t2.s where length(t1.s) % 2 = 0 format Null;

