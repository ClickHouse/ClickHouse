drop table if exists data_01256;
drop table if exists buffer_01256;

create table data_01256 as system.numbers Engine=Memory();

select 'min';
create table buffer_01256 as system.numbers Engine=Buffer(currentDatabase(), data_01256, 1,
    2, 100, /* time */
    4, 100, /* rows */
    1, 1e6  /* bytes */
);
insert into buffer_01256 select * from system.numbers limit 5;
select count() from data_01256;
-- sleep 2 (min time) + 1 (round up) + bias (1) = 4
select sleepEachRow(2) from numbers(2) FORMAT Null;
select count() from data_01256;
drop table buffer_01256;

select 'max';
create table buffer_01256 as system.numbers Engine=Buffer(currentDatabase(), data_01256, 1,
    100, 2,   /* time */
    0,   100, /* rows */
    0,   1e6  /* bytes */
);
insert into buffer_01256 select * from system.numbers limit 5;
select count() from data_01256;
-- sleep 2 (min time) + 1 (round up) + bias (1) = 4
select sleepEachRow(2) from numbers(2) FORMAT Null;
select count() from data_01256;
drop table buffer_01256;

select 'direct';
create table buffer_01256 as system.numbers Engine=Buffer(currentDatabase(), data_01256, 1,
    100, 100, /* time */
    0,   9,   /* rows */
    0,   1e6  /* bytes */
);
insert into buffer_01256 select * from system.numbers limit 10;
select count() from data_01256;

select 'drop';
insert into buffer_01256 select * from system.numbers limit 10;
drop table if exists buffer_01256;
select count() from data_01256;

drop table data_01256;
