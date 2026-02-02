drop table if exists x;

create table x engine MergeTree order by () as select () as a, () as b;

insert into x values ((), ());

select count() from x;

select * from x order by ();

select ();

drop table x;

drop table if exists x;

create table x (i Nullable(Tuple())) engine MergeTree order by (); -- { serverError 43 }
create table x (i LowCardinality(Tuple())) engine MergeTree order by (); -- { serverError 43 }
create table x (i Tuple(), j Array(Tuple())) engine MergeTree order by ();

insert into x values ((), [(), ()]), ((), []);

select count() from x;

select * from x order by () settings max_threads = 1;

drop table x;
