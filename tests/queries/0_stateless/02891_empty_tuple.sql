drop table if exists x;

create table x engine MergeTree order by () as select () as a, () as b;

insert into x values ((), ());

select count() from x;

select * from x order by ();

select ();

drop table x;
