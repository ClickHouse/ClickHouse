drop table if exists t;
create table t engine Memory as select * from numbers(2);

select count(*) from t, numbers(2) r;
select count(*) from t cross join numbers(2) r;
select count() from t cross join numbers(2) r;
select count(t.number) from t cross join numbers(2) r;
select count(r.number) from t cross join numbers(2) r;

drop table t;
