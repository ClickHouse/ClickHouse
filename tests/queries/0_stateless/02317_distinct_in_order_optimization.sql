select '-- enable distinct in order optimization';
set optimize_distinct_in_order=1;
select '-- create table with only primary key columns';
drop table if exists distinct_in_order sync;
create table distinct_in_order (a int) engine=MergeTree() order by a settings index_granularity=10;
select '-- the same values in every chunk, pre-distinct should skip entire chunks with the same key as previous one';
insert into distinct_in_order (a) select * from zeros(10);
insert into distinct_in_order (a) select * from zeros(10); -- this entire chunk should be skipped in pre-distinct
select distinct * from distinct_in_order settings max_block_size=10, max_threads=1;

select '-- create table with only primary key columns';
select '-- pre-distinct should skip part of chunk since it contains values from previous one';
drop table if exists distinct_in_order sync;
create table distinct_in_order (a int) engine=MergeTree() order by a settings index_granularity=10;
insert into distinct_in_order (a) select * from zeros(10);
insert into distinct_in_order select * from numbers(10); -- first row (0) from this chunk should be skipped in pre-distinct
select distinct a from distinct_in_order settings max_block_size=10, max_threads=1;

select '-- create table with not only primary key columns';
drop table if exists distinct_in_order sync;
create table distinct_in_order (a int, b int, c int) engine=MergeTree() order by (a, b);
insert into distinct_in_order select number % number, number % 5, number % 10 from numbers(1,1000000);

select '-- distinct with primary key prefix only';
select distinct a from distinct_in_order;
select '-- distinct with primary key prefix only, order by sorted column';
select distinct a from distinct_in_order order by a;
select '-- distinct with primary key prefix only, order by sorted column desc';
select distinct a from distinct_in_order order by a desc;

select '-- distinct with full key, order by sorted column';
select distinct a,b from distinct_in_order order by b;
select '-- distinct with full key, order by sorted column desc';
select distinct a,b from distinct_in_order order by b desc;

select '-- distinct with key prefix and non-sorted column, order by non-sorted';
select distinct a,c from distinct_in_order order by c;
select '-- distinct with key prefix and non-sorted column, order by non-sorted desc';
select distinct a,c from distinct_in_order order by c desc;

select '-- distinct with non-key prefix and non-sorted column, order by non-sorted';
select distinct b,c from distinct_in_order order by c;
select '-- distinct with non-key prefix and non-sorted column, order by non-sorted desc';
select distinct b,c from distinct_in_order order by c desc;

select '-- distinct with constants columns';
-- { echoOn }
select distinct 1 as a, 2 as b from distinct_in_order;
select distinct 1 as a, 2 as b from distinct_in_order order by a;
select distinct 1 as a, 2 as b from distinct_in_order order by a, b;
select distinct x, y from (select 1 as x, 2 as y from distinct_in_order order by x) order by x;
-- { echoOff }

drop table if exists distinct_in_order sync;

select '-- check that distinct in order has the same result as ordinary distinct';
drop table if exists distinct_cardinality_low sync;
CREATE TABLE distinct_cardinality_low (low UInt64, medium UInt64, high UInt64) ENGINE MergeTree() ORDER BY (low, medium);
INSERT INTO distinct_cardinality_low SELECT number % 1e1, number % 1e2, number % 1e3 FROM numbers_mt(1e4);

drop table if exists distinct_in_order sync;
drop table if exists ordinary_distinct sync;

create table distinct_in_order (low UInt64, medium UInt64, high UInt64) engine=MergeTree() order by (low, medium);
insert into distinct_in_order select distinct * from distinct_cardinality_low order by high settings optimize_distinct_in_order=1;
create table ordinary_distinct (low UInt64, medium UInt64, high UInt64) engine=MergeTree() order by (low, medium);
insert into ordinary_distinct select distinct * from distinct_cardinality_low order by high settings optimize_distinct_in_order=0;
select distinct * from distinct_in_order except select * from ordinary_distinct;

drop table if exists distinct_in_order;
drop table if exists ordinary_distinct;
drop table if exists distinct_cardinality_low;
