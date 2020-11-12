drop table if exists testX;
drop table if exists testXA;
drop table if exists testXB;
drop table if exists testXC;

create table testX (A Int64) engine=MergeTree order by tuple();

create materialized view testXA engine=MergeTree order by tuple() as select sleep(1) from testX;
create materialized view testXB engine=MergeTree order by tuple() as select sleep(2), throwIf(A=1) from testX;
create materialized view testXC engine=MergeTree order by tuple() as select sleep(1) from testX;

set parallel_view_processing=1;
insert into testX select number from numbers(10); -- {serverError 395}

select count() from testX;
select count() from testXA;
select count() from testXB;
select count() from testXC;

drop table testX;
drop view testXA;
drop view testXB;
drop view testXC;
