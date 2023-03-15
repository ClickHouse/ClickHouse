SET allow_experimental_analyzer = 1;

drop table if exists tab;
create table tab  (x UInt64, `arr.a` Array(UInt64), `arr.b` Array(UInt64)) engine = MergeTree order by x;
select x from tab array join arr prewhere x != 0 where arr; -- { serverError 43 }
select x from tab array join arr prewhere arr where x != 0; -- { serverError 43 }
drop table if exists tab;
