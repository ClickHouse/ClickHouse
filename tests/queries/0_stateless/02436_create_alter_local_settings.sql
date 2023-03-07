
drop table if exists t;
select toDecimal256(1, 50) / toDecimal256(1, 50);   -- { serverError DECIMAL_OVERFLOW }

set decimal_check_overflow=0;
create table t (n int, d1 Decimal256(50) materialized toDecimal256(1, 50) / toDecimal256(1, 50)) engine=MergeTree order by n;   -- { serverError DECIMAL_OVERFLOW }
create table t (n int) engine=MergeTree order by n;
insert into t values (1);

set decimal_check_overflow=0;
alter table t add column d2 Decimal256(50) materialized toDecimal256(2, 50) / toDecimal256(2, 50);   -- { serverError DECIMAL_OVERFLOW }
insert into t values (2);

detach table t;
set decimal_check_overflow=1;
attach table t;

insert into t values (3);
select * from t order by n;

drop table t;
