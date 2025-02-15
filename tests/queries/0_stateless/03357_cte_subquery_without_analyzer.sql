set enable_analyzer = 0;

create table data (id Int32) engine = MergeTree order by id;
insert into data select number from numbers(100);

create table filters (id Int32) engine = MergeTree order by id;
insert into filters select number from numbers(20);

select '-- Check subqueries are not propagated to remote';

with
    flt as (select id from filters)
select count()
from remote('127.0.0.1:9000', currentDatabase(), 'data')
where id global in (flt)
settings prefer_localhost_replica = 0,
         log_comment = '03357_global_in_subquery';

select '';
system flush logs;

with (
    select query_id from system.query_log
    where log_comment = '03357_global_in_subquery'
      and type = 'QueryStart'
      and current_database = currentDatabase()
      and is_initial_query
) as initial_id
select count(), countIf(startsWith(lower(query), 'with flt'))
from system.query_log
where initial_query_id = initial_id
  and type = 'QueryStart'
  and query_id != initial_query_id;

select '';
select '-- Check scalar subqueries are propagated to remote';

with
    (select count() from filters) as flt
select count()
from remote('127.0.0.1:9000', currentDatabase(), 'data')
where id global in (flt)
settings prefer_localhost_replica = 0,
         log_comment = '03357_global_in_scalar';

select '';
system flush logs;

with (
    select query_id from system.query_log
    where log_comment = '03357_global_in_scalar'
      and type = 'QueryStart'
      and current_database = currentDatabase()
      and is_initial_query
) as initial_id
select count(), countIf(startsWith(lower(query), 'with _cast(20,'))
from system.query_log
where initial_query_id = initial_id
  and type = 'QueryStart'
  and query_id != initial_query_id;

select '';
select '-- Check expressions are propagated to remote';

with count() as expr
select expr
from remote('127.0.0.1:9000', currentDatabase(), 'data')
where id in (10, 20)
settings prefer_localhost_replica = 0,
         log_comment = '03357_expressions';

select '';
system flush logs;

with (
    select query_id from system.query_log
    where log_comment = '03357_expressions'
      and type = 'QueryStart'
      and current_database = currentDatabase()
      and is_initial_query
) as initial_id
select count(), countIf(startsWith(lower(query), 'with count() as `expr`'))
from system.query_log
where initial_query_id = initial_id
  and type = 'QueryStart'
  and query_id != initial_query_id;

select '';
select '-- Check only subqueries are not propagated to remote';

with count() as expr,
     (select count() from filters where id in (5, 15)) as sclr,
     sub as (select * from filters where id >= 10)
select expr
from remote('127.0.0.1:9000', currentDatabase(), 'data')
where id global in (sclr)
   or id global in (sub)
settings prefer_localhost_replica = 0,
         log_comment = '03357_mixed';

select '';
system flush logs;

with (
    select query_id from system.query_log
    where log_comment = '03357_mixed'
      and type = 'QueryStart'
      and current_database = currentDatabase()
      and is_initial_query
) as initial_id
select count(),
       countIf(lower(query) like '%(select * from filters where id < 10)%'),
       countIf(startsWith(lower(query), 'with'))
from system.query_log
where initial_query_id = initial_id
  and type = 'QueryStart'
  and query_id != initial_query_id;
