drop table if exists x;

create table x engine MergeTree order by () as select () as a, () as b; -- { serverError 370 }

select ();

select number from numbers(10) order by ();
