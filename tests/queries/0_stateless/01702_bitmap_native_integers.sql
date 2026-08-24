drop table if exists t;
create table t(i8 Int8, i16 Int16, i32 Int32, i64 Int64) engine Memory;
insert into t values (-1, -1, -1, -1), (-2, -2, -2, -2), (-3, -3, -3, -3), (-4, -4, -4, -4), (-5, -5, -5, -5);
select * apply bitmapMin, * apply bitmapMax from (select * apply groupBitmapState from t);
drop table t;
