create temporary table t1 (a Nullable(UInt8));
insert into t1 values (2.4);
select * from t1;

create temporary table t2 (a UInt8);
insert into t2 values (2.4);
select * from t2;
