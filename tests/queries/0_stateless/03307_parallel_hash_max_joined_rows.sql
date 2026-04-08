-- Tags: no-tsan, no-asan, no-msan, no-ubsan, no-sanitize-coverage, no-parallel-replicas
-- no sanitizers -- memory consumption is unpredicatable with sanitizers

drop table if exists t;

create table t(s String, n UInt8) Engine = MergeTree order by tuple();
insert into t select repeat('x', 100) as s, number % 85 from numbers_mt(1e5);

set max_result_rows = 0, max_result_bytes = 0, max_block_size = 65409, max_threads = 32, join_algorithm = 'parallel_hash';
set max_memory_usage = '5Gi';

select * from t t1 join t t2 on t1.n = t2.n format Null settings max_joined_block_size_rows = 65409;
