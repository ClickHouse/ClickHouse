-- { echo ON }

drop table if exists t;

create table t(A Int64) partition by (A % 64) order by A desc settings allow_experimental_reverse_key=1
as select intDiv(number,11111) from numbers(7e5) union all select number from numbers(7e5);

set max_threads=1;

select cityHash64(groupArray(A)) from (select A from t order by A desc limit 10);

select cityHash64(groupArray(A))  from (select A from t order by identity(A) desc limit 10);

drop table t;
