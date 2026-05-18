set async_insert = 1;
set wait_for_async_insert = 0;
set async_insert_deduplicate = 1;
set deduplicate_blocks_in_dependent_materialized_views = 1;

set async_insert_use_adaptive_busy_timeout=0, async_insert_busy_timeout_min_ms=1000, async_insert_busy_timeout_max_ms=5000;

create table src_table
(
    id   UInt32,
    name String
)
engine = ReplicatedMergeTree('/clickhouse/tables/test/{database}/table', 'replica1')
order by id;

create table table_join
(
    id   UInt32,
    surname String
)
engine = ReplicatedMergeTree('/clickhouse/tables/test/{database}/table_join', 'replica1')
order by id;

set max_block_size = 30000;
set max_insert_block_size = 30000;
set min_insert_block_size_rows = 30000;

insert into table_join select 1 as id, toString(number) from numbers(10000);
insert into table_join select 2 as id, toString(number) from numbers(10000);
insert into table_join select 3 as id, toString(number) from numbers(10000);
insert into table_join select 4 as id, toString(number) from numbers(10000);

system flush async insert queue 03733_table_join;
select 'chech table_join size';
select count(*) from table_join;  -- Expecting 400000

create table table_join_mv_dst
(
    id      UInt32,
    name    String,
    surname String
)
engine = ReplicatedMergeTree('/clickhouse/tables/test/{database}/table_join_mv_dst', 'replica1')
order by id;

create materialized view table_join_mv
TO table_join_mv_dst
as select t1.id as id, t1.name as name, t2.surname as surname from src_table as t1 join table_join as t2 using id;

select 'insert with self deduplication with join inside mv which produces sutable result';
insert into src_table values (1, 'Alice');
insert into src_table values (2, 'Bob');
insert into src_table values (1, 'Alice');
system flush async insert queue src_table;
select count(*) from src_table;             -- Expecting 2
select count(*) from table_join_mv_dst; -- Expecting 20000

select 'insert with self deduplication with join inside mv which produces large result';
insert into src_table values (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'David');
insert into src_table values (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'David');
insert into src_table values (1, 'Alice'), (1, 'Alice'), (1, 'Alice'), (1, 'Alice');
system flush async insert queue src_table;

system flush logs system.query_log;
select query, type, exception_code from system.query_log
where
    has(databases, currentDatabase())
    and has(tables, currentDatabase() || '.src_table')
    and type != 'QueryStart'
    and query_kind = 'AsyncInsertFlush'
order by all desc;
