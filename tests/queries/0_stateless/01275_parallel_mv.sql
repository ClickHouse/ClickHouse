set max_threads = 0;

drop table if exists testX;
drop table if exists testXA;
drop table if exists testXB;
drop table if exists testXC;

create table testX (A Int64) engine=MergeTree order by tuple();

create materialized view testXA engine=MergeTree order by tuple() as select sleep(1) from testX;
create materialized view testXB engine=MergeTree order by tuple() as select sleep(2), throwIf(A=1) from testX;
create materialized view testXC engine=MergeTree order by tuple() as select sleep(1) from testX;

-- { echoOn }
set parallel_view_processing=1;
insert into testX select number from numbers(10) settings log_queries=1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
system flush logs;
select length(thread_ids) >= 8 from system.query_log where current_database = currentDatabase() and type != 'QueryStart' and query like '%insert into testX %' and Settings['parallel_view_processing'] = '1';

select count() from testX;
select count() from testXA;
select count() from testXB;
select count() from testXC;

set parallel_view_processing=0;
insert into testX select number from numbers(10) settings log_queries=1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
system flush logs;
select length(thread_ids) >= 5 from system.query_log where current_database = currentDatabase() and type != 'QueryStart' and query like '%insert into testX %' and Settings['parallel_view_processing'] = '0';

select count() from testX;
select count() from testXA;
select count() from testXB;
select count() from testXC;
-- { echoOff }

drop table testX;
drop view testXA;
drop view testXB;
drop view testXC;
