drop table if exists mt1;
drop table if exists mt2;

create table mt1 (n Int64) engine=MergeTree order by n;
create table mt2 (n Int64) engine=MergeTree order by n;

commit; -- { serverError 645 }
rollback; -- { serverError 645 }

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
begin transaction; -- { serverError 645 }
rollback;

begin transaction;
insert into mt1 values (3);
insert into mt2 values (30);
select 'on exception before start', arraySort(groupArray(n)) from (select n from mt1 union all select * from mt2);
-- rollback on exception before start
select functionThatDoesNotExist(); -- { serverError 46 }
-- cannot commit after exception
commit; -- { serverError 645 }
begin transaction; -- { serverError 645 }
rollback;

begin transaction;
insert into mt1 values (4);
insert into mt2 values (40);
select 'on exception while processing', arraySort(groupArray(n)) from (select n from mt1 union all select * from mt2);
-- rollback on exception while processing
select throwIf(100 < number) from numbers(1000); -- { serverError 395 }
-- cannot commit after exception
commit; -- { serverError 645 }
insert into mt1 values (5); -- { serverError 645 }
insert into mt2 values (50); -- { serverError 645 }
select 1; -- { serverError 645 }
rollback;

begin transaction;
insert into mt1 values (6);
insert into mt2 values (60);
select 'on session close', arraySort(groupArray(n)) from (select n from mt1 union all select * from mt2);
-- trigger reconnection by error on client, check rollback on session close
insert into mt1 values ([1]); -- { clientError 43 }
commit; -- { serverError 645 }
rollback; -- { serverError 645 }

begin transaction;
insert into mt1 values (7);
insert into mt2 values (70);
select 'commit', arraySort(groupArray(n)) from (select n from mt1 union all select * from mt2);
commit;

begin transaction;
select 'readonly', arraySort(groupArray(n)) from (select n from mt1 union all select * from mt2);
commit;

begin transaction;
create table m (n int) engine=Memory; -- { serverError 48 }
commit; -- { serverError 645 }
rollback;

create table m (n int) engine=Memory;
begin transaction;
insert into m values (1); -- { serverError 48 }
select * from m; -- { serverError 645 }
commit; -- { serverError 645 }
rollback;

begin transaction;
select * from m; -- { serverError 48 }
commit; -- { serverError 645 }
rollback;

drop table m;
drop table mt1;
drop table mt2;
