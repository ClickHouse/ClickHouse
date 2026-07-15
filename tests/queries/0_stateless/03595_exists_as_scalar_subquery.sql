set enable_analyzer=1;
drop table if exists tab;

create table tab (id Int32) engine = MergeTree order by id;
insert into tab values (1), (2), (3);

set force_primary_key = 1;

select * from tab where id > 0 or exists (select number from numbers(10) where number > 10) or exists (select number from numbers(10) where number > 10) settings execute_exists_as_scalar_subquery = 0; -- { serverError INDEX_NOT_USED }
select * from tab where id > 2 or exists (select number from numbers(10) where number > 10) or exists (select number from numbers(10) where number > 10) settings execute_exists_as_scalar_subquery = 1;

set force_primary_key = 0;
system flush logs query_log;
SELECT 'ScalarSubqueriesGlobalCacheHit ' || ProfileEvents['ScalarSubqueriesGlobalCacheHit'] FROM system.query_log
WHERE type != 'QueryStart' AND current_database = currentDatabase() AND query like 'select%' AND Settings['execute_exists_as_scalar_subquery']='1';

SELECT EXISTS(SELECT 1 from numbers(2) where number != 0) settings execute_exists_as_scalar_subquery = 1;
