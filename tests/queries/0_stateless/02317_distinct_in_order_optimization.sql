drop table if exists distinct_in_order sync;
create table distinct_in_order (a int, b int, c int) engine=MergeTree() order by (a, b, c);

select 'enable optimize_distinct_in_order';
set optimize_distinct_in_order=1;
select 'distinct pipeline on empty table -> no optimization, source is ReadFromPreparedSource instead of ReadFromMergeTree';
explain pipeline select distinct * from distinct_in_order settings max_threads=1;

select 'insert into table to use ReadFromMergeTree source';
insert into distinct_in_order select number % number, number % 10, number % 5 from numbers(1,10);

select 'disable optimize_distinct_in_order';
set optimize_distinct_in_order=0;
select 'pipeline does _not_ contain the optimization';
explain pipeline select distinct * from distinct_in_order settings max_threads=1;

select 'enable optimize_distinct_in_order';
set optimize_distinct_in_order=1;
select 'distinct with all primary key columns -> optimization applied';
insert into distinct_in_order select number % number, number % 10, number % 5 from numbers(1,10);
explain pipeline select distinct * from distinct_in_order settings max_threads=1;
select 'distinct with primary key prefix -> optimization applied';
explain pipeline select distinct a, c from distinct_in_order settings max_threads=1;
select 'distinct with non-primary key prefix -> no optimization';
explain pipeline select distinct b, c from distinct_in_order settings max_threads=1;

select 'the same values in every chunk, distinct in order should skip entire chunks with the same key as previous one';
drop table if exists distinct_in_order sync;
create table distinct_in_order (a int) engine=MergeTree() order by a settings index_granularity=10;
insert into distinct_in_order (a) select * from zeros(30);
select 'single-threaded distinct';
select distinct * from distinct_in_order settings max_block_size=10, max_threads=1;
select 'multi-threaded distinct';
select distinct * from distinct_in_order settings max_block_size=10;

select 'skip part of chunk since it contains values from previous one';
drop table if exists distinct_in_order sync;
create table distinct_in_order (a int) engine=MergeTree() order by a settings index_granularity=10;
insert into distinct_in_order (a) select * from zeros(10);
insert into distinct_in_order select * from numbers(10);
select 'single-threaded distinct';
select distinct a from distinct_in_order settings max_block_size=10, max_threads=1;
select 'multi-threaded distinct';
select distinct a from distinct_in_order settings max_block_size=10;

select 'table with not only primary key columns';
drop table if exists distinct_in_order sync;
create table distinct_in_order (a int, b int, c int) engine=MergeTree() order by (a, b);
insert into distinct_in_order select number % number, number % 10, number % 5 from numbers(1,1000000);
select 'distinct with key-prefix only';
select distinct a from distinct_in_order;
select 'distinct with full key';
select distinct a,b from distinct_in_order order by b;
select 'distinct with key prefix and non-sorted column';
select distinct a,c from distinct_in_order order by c;

-- drop table if exists distinct_in_order sync;
