set async_insert = 1;
set wait_for_async_insert = 0;
set async_insert_deduplicate = 1;
set deduplicate_blocks_in_dependent_materialized_views = 1;
set throw_if_deduplication_in_dependent_materialized_views_enabled_with_async_insert = 0;

set async_insert_use_adaptive_busy_timeout=0, async_insert_busy_timeout_min_ms=1000, async_insert_busy_timeout_max_ms=5000;

create table src_table
(
    id   UInt32,
    name String
)
engine = MergeTree
order by id
SETTINGS non_replicated_deduplication_window = 10000;

insert into src_table values (1, 'Alice'), (2, 'Bob'), (1, 'Alice');
insert into src_table values (1, 'Alice');
insert into src_table values (2, 'Bob');
insert into src_table values (1, 'Alice');
system flush async insert queue src_table;

-- Expecting 2
select 'src_table', count(*) from src_table;

create table dst_1_0
(
    id      UInt32,
    name    String
)
engine = MergeTree
order by id
SETTINGS non_replicated_deduplication_window = 10000;
create materialized view mv_1_0
TO dst_1_0
as select * from src_table where id % 2 = 0;

create table dst_1_1
(
    id      UInt32,
    name    String
)
engine = MergeTree
order by id
SETTINGS non_replicated_deduplication_window = 10000;
create materialized view mv_1_1
TO dst_1_1
as select * from src_table where id % 2 = 1;

create table dst_1_2
(
    id      UInt32,
    name    String
)
engine = MergeTree
order by id
SETTINGS non_replicated_deduplication_window = 10000;
create materialized view mv_1_2
TO dst_1_2
as select * from src_table;

create table dst_2_01
(
    id      UInt32,
    name    String
)
engine = MergeTree
order by id
SETTINGS non_replicated_deduplication_window = 10000;

create materialized view mv_2_00
TO dst_2_01
as select * from dst_1_0;

create materialized view mv_2_01
TO dst_2_01
as select * from dst_1_1;

insert into src_table values (1, 'Alice'), (2, 'Bob'), (1, 'Alice');
insert into src_table values (3, 'Charlie'), (4, 'David'), (3, 'Charlie');
insert into src_table values (1, 'Alice'), (2, 'Bob'), (1, 'Alice');
system flush async insert queue src_table;

-- Expecting 8
select 'src_table', count(*) from src_table;
select * from src_table order by all;
-- Expecting 2
select 'dst_1_0', count(*) from dst_1_0;
select * from dst_1_0 order by all;
-- Expecting 4
select 'dst_1_1', count(*) from dst_1_1;
select * from dst_1_1 order by all;
-- Expecting 6
select 'dst_1_2', count(*) from dst_1_2;
select * from dst_1_2 order by all;
-- Expecting 6
select 'dst_2_01', count(*) from dst_2_01;
select * from dst_2_01 order by all;

system flush logs system.query_log;
select query, query_kind, exception_code,
    read_rows, written_rows,
    ProfileEvents['QueriesWithSubqueries'] as QueriesWithSubqueries,
    ProfileEvents['SelectQueriesWithSubqueries'] as SelectQueriesWithSubqueries,
    ProfileEvents['AsyncInsertRows'] as AsyncInsertRows,
    ProfileEvents['SelfDuplicatedAsyncInserts'] as SelfDuplicatedAsyncInserts,
    ProfileEvents['DuplicatedAsyncInserts'] as DuplicatedAsyncInserts
from system.query_log
where
    has(databases, currentDatabase())
    and has(tables, currentDatabase() || '.src_table')
    and type != 'QueryStart'
    and query_kind = 'AsyncInsertFlush'
order by all desc FORMAT Vertical;
