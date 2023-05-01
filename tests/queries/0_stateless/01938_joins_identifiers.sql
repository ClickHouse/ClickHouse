DROP TABLE IF EXISTS "/t0";
DROP TABLE IF EXISTS "/t1";

create table "/t0" (a Int64, b Int64) engine = MergeTree() partition by a order by a;
create table "/t1" (a Int64, b Int64) engine = MergeTree() partition by a order by a;

insert into "/t0" values (0, 0);
insert into "/t1" values (0, 1);

select * from "/t0" join "/t1" using a;

DROP TABLE "/t0";
DROP TABLE "/t1";
