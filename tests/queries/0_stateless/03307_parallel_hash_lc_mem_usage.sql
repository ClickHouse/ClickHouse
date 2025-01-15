-- Tags: no-random-settings, no-random-merge-tree-settings, no-tsan, no-asan, no-msan, no-ubsan
-- no sanitizers -- memory consumption is unpredicatable with sanitizers

drop table if exists t;
create table t(s String, s2 String) Engine = MergeTree order by tuple() settings index_granularity = 100;

insert into t select repeat('x', number%100) as s, s as s2 from numbers_mt(3e5);

set max_threads = 32, max_memory_usage = '2Gi', join_algorithm = 'parallel_hash';

select * from t t1 join t t2 on t1.s = t2.s where length(t1.s) % 2 = 0 format Null;
