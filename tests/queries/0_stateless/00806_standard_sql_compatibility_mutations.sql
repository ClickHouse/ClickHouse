set mutations_sync=1;
drop table if exists t1;
create table t1(id Int32, name String, d Date)
    Engine = MergeTree() partition by d order by id;

insert into t1 values(1, 'a', '2018-01-01'), (2, 'b', '2018-01-02'), (3, 'c', '2018-01-03');

update t1 set name ='a1'  where id =1;
select name from t1 where id=1;
update t1 set name ='e';
select name from t1;

delete from t1 where id=1;
select count(*) from t1 where id=1;
delete from t1;
select count(*) from t1;

drop table t1;