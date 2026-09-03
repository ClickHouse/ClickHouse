
drop table if exists t;
drop table if exists mv;

create table t engine=Memory empty; -- { clientError SYNTAX_ERROR }
create table t engine=Memory empty as; -- { clientError SYNTAX_ERROR }
create table t engine=Memory as; -- { clientError SYNTAX_ERROR }
create table t engine=Memory empty as select 1;

show create table t;
select count() from t;

create materialized view mv engine=Memory empty as select 1;
show create mv;
select count() from mv;

drop table t;
drop table mv;
