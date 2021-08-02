drop table if exists src;
drop table if exists src_am;
create table src (x UInt32, y String) engine = MergeTree order by x;

--- Basic syntax

-- Query for AggregatingMemory must have aggregation
create table src_am engine = AggregatingMemory as select 1; -- { serverError 80 }

--- AS SELECT from table

-- Aggregation without key
create table src_am engine = AggregatingMemory as select sum(x) from src;
-- Create query contains table structure for insert (so that we know table structure from attach)
-- Source table is not shown in create query.
show create table src_am;
-- In fact, we don't need src table anymore.
drop table src;
-- Table structure looks like query result after aggregation.
desc table src_am;
-- Now, we can insert data with compatible structure (it's only `x UInt32`)
insert into src_am values (1), (2);
insert into src_am values (3), (4);
-- Select returns aggregated result
select * from src_am;
-- Result column is `sum(x)`, and it's an identifier.
select `sum(x)` from src_am;
-- Just sum(x) will not work. Use backtips or add alias to initial query.
select sum(x) from src_am; -- { serverError 47 }

-- Aggregation with key
drop table src_am;
create table src (x UInt32, y String) engine = MergeTree order by x;
create table src_am engine = AggregatingMemory as select sum(x) as s, y from src group by y;
drop table src;
-- x, y
show create table src_am;
-- s, y
desc table src_am;
insert into src_am values (1, 'a'), (2, 'b');
insert into src_am values (3, 'a'), (4, 'b');
select * from src_am order by y;
select s, y from src_am order by y;
select y from src_am order by y;
-- There is no column x anymore
select sum(x), y from src_am; -- { serverError 47 }
select `sum(x)`, y from src_am; -- { serverError 47 }

-- Check detach/attach works (note: src table is dropped)
detach table src_am;
attach table src_am;
-- Should be the same as before
show create table src_am;
desc table src_am;
-- Now, table is empty
select * from src_am order by y;
-- Reinsert data
insert into src_am values (1, 'a'), (2, 'b');
insert into src_am values (3, 'a'), (4, 'b');
select * from src_am order by y;

--- With specified table structure
drop table src_am;
-- If we specify table structure, source table is expected to have this structure.
-- Table does not have to exist.
create table src_am (x UInt32, y String) engine = AggregatingMemory as select sum(x) as s, y from non_existing_table group by y;
-- Src table is deleted from creating query.
show create table src_am;
desc table src_am;
insert into src_am values (1, 'a'), (2, 'b');
insert into src_am values (3, 'a'), (4, 'b');
select * from src_am order by y;
-- In fact, we can skip src table name.
drop table src_am;
create table src_am (x UInt32, y String) engine = AggregatingMemory as select sum(x) as s, y group by y;
insert into src_am values (1, 'a'), (2, 'b');
insert into src_am values (3, 'a'), (4, 'b');
select * from src_am order by y;

--- MV to AggregatingMemory.
create table src (a UInt32, b String) engine = MergeTree order by a;
create materialized view mv_am to src_am as select a + 1 as x, b || '_' as y from src;
-- MV should have the same structure as DESC src_am.
show create table mv_am;
desc table mv_am;
-- Old data.
select * from mv_am order by y;
insert into src values (1, 'a'), (2, 'b');
insert into src values (3, 'a'), (4, 'b');
-- New data should be transformed by both queries.
select * from mv_am order by y;
-- Same result.
select * from src_am order by y;

detach table src_am;
detach table mv_am;
-- MV could be attached before src_am table.
attach table mv_am;
-- MV structure should not be changed.
show create table mv_am;
desc table mv_am;
-- But cannot read without src_am.
select * from src_am order by y; -- { serverError 60 }
attach table src_am;
-- Now ok, but result is empty.
select * from src_am order by y;

--- MV with inner AggregatingMemory.
drop table src;
drop table mv_am;
create table src (x UInt32, y String) engine = MergeTree order by x;
create table mv_am engine = AggregatingMemory as select sum(x), y from src group by y;
-- Structure for MV is the same as for inner table.
show create table mv_am;
desc table mv_am;
show create table `.inner.mv_am`;
desc table `.inner.mv_am`;
