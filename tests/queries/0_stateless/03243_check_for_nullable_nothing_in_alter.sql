drop table if exists src;
drop table if exists dst;
drop view if exists v;
create table src (x Nullable(Int32)) engine=Memory;
alter table src modify column x Nullable(Nothing); -- {serverError DATA_TYPE_CANNOT_BE_USED_IN_TABLES}
create table dst (x Nullable(Int32)) engine=Memory;
create materialized view v to dst as select x from src;
alter table v modify query select NULL as x from src; -- {serverError DATA_TYPE_CANNOT_BE_USED_IN_TABLES}
drop view v;
drop table dst;
drop table src;

