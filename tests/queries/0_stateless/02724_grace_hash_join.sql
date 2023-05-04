set join_algorithm = 'grace_hash';
set max_rows_in_join = 100;

drop table if exists test_t1;
drop table if exists test_t2;

CREATE TABLE test_t1
(
    `x` UInt64,
    `y` Nullable(UInt64)
)
ENGINE = Memory;

CREATE TABLE test_t2
(
    `x` UInt64,
    `y` Nullable(UInt64)
)
ENGINE = Memory;

insert into test_t1 select number as x, number % 20 as y from numbers(200) where number > 50 order by y;
insert into test_t2 select number as x , number % 30 as y from numbers(300) order by y;

select 'inner join 1';
select l.x, l.y, r.y from test_t1 as l inner join test_t2 as r on l.x = r.x order by l.x, l.y, r.y;
select 'inner join 2';
select l.x, l.y, r.y from test_t2 as l inner join test_t1 as r on l.x = r.x order by l.x, l.y, r.y;
select 'left join 1';
select l.x, l.y, r.y from test_t1 as l left join test_t2 as r on l.x = r.x order by l.x, l.y, r.y;
select 'left join 2';
select l.x, l.y, r.y from test_t2 as l left join test_t1 as r on l.x = r.x order by l.x, l.y, r.y;
select 'right join 1';
select l.x, l.y, r.y from test_t1 as l right join test_t2 as r on l.x = r.x order by l.x, l.y, r.y;
select 'right join 2';
select l.x, l.y, r.y from test_t2 as l right join test_t1 as r on l.x = r.x order by l.x, l.y, r.y;
select 'full join 1';
select l.x, l.y, r.y from test_t1 as l full join test_t2 as r on l.x = r.x order by l.x, l.y, r.y;
select 'full join 2';
select l.x, l.y, r.y from test_t2 as l full join test_t1 as r on l.x = r.x order by l.x, l.y, r.y;





