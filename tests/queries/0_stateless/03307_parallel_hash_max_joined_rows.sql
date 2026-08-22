-- Tags: no-tsan, no-asan, no-msan, no-ubsan, no-sanitize-coverage, no-parallel-replicas
-- no sanitizers -- memory consumption is unpredicatable with sanitizers

drop table if exists t;

create table t(s String, n UInt8) Engine = MergeTree order by tuple();
insert into t select repeat('x', 100) as s, number from numbers_mt(3e5);

set max_result_rows = 0, max_result_bytes = 0, max_block_size = 65409, max_threads = 32, join_algorithm = 'parallel_hash';
set max_memory_usage = '5Gi'; -- on my machine with max_joined_block_size_rows=65K I see consumption of ~1G,
                              -- without this limit (i.e. max_joined_block_size_rows=0) consumption is ~8-10G

select * from t t1 join t t2 on t1.n = t2.n format Null settings max_joined_block_size_rows = 65409;
