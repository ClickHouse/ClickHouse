drop table if exists distinct_in_order sync;
create table distinct_in_order (a int, b int, c int) engine=MergeTree() order by (a, b, c);

select '-- enable optimize_distinct_in_order';
set optimize_distinct_in_order=1;
select '-- distinct pipeline on empty table -> no optimization, table is empty';
explain pipeline select distinct * from distinct_in_order settings max_threads=1;

select '-- insert into table';
insert into distinct_in_order select number % number, number % 5, number % 10 from numbers(1,10);

select '-- disable optimize_distinct_in_order';
set optimize_distinct_in_order=0;
select '-- distinct all primary key columns -> no optimization';
explain pipeline select distinct * from distinct_in_order settings max_threads=1;

select '-- enable optimize_distinct_in_order';
set optimize_distinct_in_order=1;

select '-- distinct with all primary key columns -> pre-distinct optimization only';
explain pipeline select distinct * from distinct_in_order settings max_threads=1;

select '-- distinct with primary key prefix -> pre-distinct optimization only';
explain pipeline select distinct a, c from distinct_in_order settings max_threads=1;

select '-- distinct with primary key prefix and order by on column in distinct -> pre-distinct and final distinct optimization';
explain pipeline select distinct a, c from distinct_in_order order by c settings max_threads=1;

select '-- distinct with primary key prefix and order by on column _not_ in distinct -> pre-distinct optimization only';
explain pipeline select distinct a, c from distinct_in_order order by b settings max_threads=1;

select '-- distinct with non-primary key prefix -> no optimization';
explain pipeline select distinct b, c from distinct_in_order settings max_threads=1;

select '-- distinct with non-primary key prefix and order by on column in distinct -> final distinct optimization only';
explain pipeline select distinct b, c from distinct_in_order order by b settings max_threads=1;

select '-- distinct with non-primary key prefix and order by on column _not_ in distinct -> no optimization';
set optimize_read_in_order = 1; -- to avoid flaky test, in this query (Sorting) step depends on the setting
explain pipeline select distinct b, c from distinct_in_order order by a settings max_threads=1;

select '-- the same values in every chunk, distinct in order should skip entire chunks with the same key as previous one';
drop table if exists distinct_in_order sync;
create table distinct_in_order (a int) engine=MergeTree() order by a settings index_granularity=10;
insert into distinct_in_order (a) select * from zeros(100);
select '-- single-threaded distinct';
select distinct * from distinct_in_order settings max_block_size=10, max_threads=1;
select '-- multi-threaded distinct';
select distinct * from distinct_in_order settings max_block_size=10;

select '-- skip part of chunk since it contains values from previous one';
drop table if exists distinct_in_order sync;
create table distinct_in_order (a int) engine=MergeTree() order by a settings index_granularity=10;
insert into distinct_in_order (a) select * from zeros(10);
insert into distinct_in_order select * from numbers(10);
select '-- single-threaded distinct';
select distinct a from distinct_in_order settings max_block_size=10, max_threads=1;
select '-- multi-threaded distinct';
select distinct a from distinct_in_order settings max_block_size=10;

select '-- create table with not only primary key columns';
drop table if exists distinct_in_order sync;
create table distinct_in_order (a int, b int, c int) engine=MergeTree() order by (a, b);
insert into distinct_in_order select number % number, number % 5, number % 10 from numbers(1,1000000);
select '-- distinct with primary key prefix only';
select distinct a from distinct_in_order;
select '-- distinct with full key';
select distinct a,b from distinct_in_order order by b;
select '-- distinct with key prefix and non-sorted column';
select distinct a,c from distinct_in_order order by c;

-- drop table if exists distinct_in_order sync;
