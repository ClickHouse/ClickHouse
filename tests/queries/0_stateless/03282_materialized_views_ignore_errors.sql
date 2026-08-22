-- more blocks to process
set max_block_size = 10;
set min_insert_block_size_rows = 10;

drop table if exists testX;
drop table if exists testXA;

create table testX (A Int64) engine=MergeTree partition by (intDiv(A, 10), throwIf(A=2)) order by tuple();
create materialized view testXA engine=MergeTree order by tuple() as select sleep(0.1) from testX;

-- { echoOn }

insert into testX select number from numbers(20)
    settings materialized_views_ignore_errors = 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

select count() from testX;
select count() from testXA;

insert into testX select number from numbers(20)
    settings materialized_views_ignore_errors = 1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

select count() from testX;
select count() from testXA;

-- { echoOff }

drop table testX;
drop view testXA;
