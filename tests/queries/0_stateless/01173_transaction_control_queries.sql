-- Tags: no-ordinary-database

drop table if exists mt1;
drop table if exists mt2;

create table mt1 (n Int64) engine=MergeTree order by n;
create table mt2 (n Int64) engine=MergeTree order by n;

commit; -- { serverError INVALID_TRANSACTION } -- no transaction
rollback; -- { serverError INVALID_TRANSACTION }

begin transaction;
insert into mt1 values (1);
insert into mt2 values (10);
select 'commit', arraySort(groupArray(n)) from (select n from mt1 union all select * from mt2);
commit;

begin transaction;
insert into mt1 values (2);
insert into mt2 values (20);
select 'rollback', arraySort(groupArray(n)) from (select n from mt1 union all select * from mt2);
rollback;

begin transaction;
select 'no nested', arraySort(groupArray(n)) from (select n from mt1 union all select * from mt2);
begin transaction; -- { serverError INVALID_TRANSACTION }
rollback;

begin transaction;
insert into mt1 values (3);
insert into mt2 values (30);
select 'on exception before start', arraySort(groupArray(n)) from (select n from mt1 union all select * from mt2);
-- rollback on exception before start
select functionThatDoesNotExist(); -- { serverError UNKNOWN_FUNCTION }
-- cannot commit after exception
commit; -- { serverError INVALID_TRANSACTION } -- after 46
begin transaction; -- { serverError INVALID_TRANSACTION }
rollback;

begin transaction;
insert into mt1 values (4);
insert into mt2 values (40);
select 'on exception while processing', arraySort(groupArray(n)) from (select n from mt1 union all select * from mt2);
-- rollback on exception while processing
select throwIf(100 < number) from numbers(1000); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
-- cannot commit after exception
commit; -- { serverError INVALID_TRANSACTION } -- after 395
insert into mt1 values (5); -- { serverError INVALID_TRANSACTION }
insert into mt2 values (50); -- { serverError INVALID_TRANSACTION }
select 1; -- { serverError INVALID_TRANSACTION }
rollback;

begin transaction;
insert into mt1 values (6);
insert into mt2 values (60);
select 'on session close', arraySort(groupArray(n)) from (select n from mt1 union all select * from mt2);
insert into mt1 values ([1]); -- { clientError ILLEGAL_TYPE_OF_ARGUMENT }
-- INSERT failures does not produce client reconnect anymore, so rollback can be done
rollback;

begin transaction;
insert into mt1 values (7);
insert into mt2 values (70);
select 'commit', arraySort(groupArray(n)) from (select n from mt1 union all select * from mt2);
commit;

begin transaction;
select 'readonly', arraySort(groupArray(n)) from (select n from mt1 union all select * from mt2);
commit;

begin transaction;
select 'snapshot', count(), sum(n) from mt1;
set transaction snapshot 1;
select 'snapshot1', count(), sum(n) from mt1;
set transaction snapshot 3;
set throw_on_unsupported_query_inside_transaction=0;
select 'snapshot3', count() = (select count() from system.parts where database=currentDatabase() and table='mt1' and _state in ('Active', 'Outdated')) from mt1;
set throw_on_unsupported_query_inside_transaction=1;
set transaction snapshot 1000000000000000;
select 'snapshot100500', count(), sum(n) from mt1;
set transaction snapshot 5; -- { serverError INVALID_TRANSACTION }
rollback;

begin transaction;
create table m (n int) engine=Memory; -- { serverError NOT_IMPLEMENTED }
commit; -- { serverError INVALID_TRANSACTION } -- after 48
rollback;

create table m (n int) engine=Memory;
begin transaction;
insert into m values (1); -- { serverError NOT_IMPLEMENTED }
select * from m; -- { serverError INVALID_TRANSACTION }
commit; -- { serverError INVALID_TRANSACTION } -- after 48
rollback;

begin transaction;
select * from m; -- { serverError NOT_IMPLEMENTED }
commit; -- { serverError INVALID_TRANSACTION } -- after 48
rollback;

drop table m;
drop table mt1;
drop table mt2;
