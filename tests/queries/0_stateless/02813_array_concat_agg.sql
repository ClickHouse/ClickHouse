drop table if exists t;

create table t (n UInt32, a Array(Int32)) engine=Memory;
insert into t values (1, [1,2,3]), (2, [4,5]), (3, [6]);

select array_concat_agg(a) from t;
select ArrAy_cOncAt_aGg(a) from t;
select n, array_concat_agg(a) from t group by n order by n;
drop table t;
