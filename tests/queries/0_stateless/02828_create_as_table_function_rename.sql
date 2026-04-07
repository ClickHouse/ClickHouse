
drop table if exists t1;
create table t1 as remote('localhost', 'system.one');
rename table t1 to t2;
select * from t2;
rename table t2 to t1;
drop table t1;
