-- Tags: long, no-fasttest

-- An INSERT that scatters a single block into a huge number of single-partition parts must stay
-- responsive to `max_execution_time` (and `KILL QUERY`) instead of running an uninterruptible loop
-- inside `MergeTreeSink::consume`. This reproduces a stress-test hung check found by the AST fuzzer:
-- an INSERT into a table with a high-cardinality `partition by` and
-- `throw_on_max_partitions_per_insert_block = 0` used to write all the parts without ever checking
-- the time limit, hanging the query (and the server shutdown) for many minutes.

drop table if exists t_many_partitions_cancel;
create table t_many_partitions_cancel (key Int) engine = MergeTree order by key partition by key;

insert into t_many_partitions_cancel select * from numbers(1000000)
settings max_insert_block_size = 1000000, max_partitions_per_insert_block = 1,
    throw_on_max_partitions_per_insert_block = 0, max_execution_time = 1, timeout_overflow_mode = 'throw'; -- { serverError TIMEOUT_EXCEEDED }

drop table t_many_partitions_cancel;
