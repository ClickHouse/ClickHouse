drop table if exists test.limit_by;

create table test.limit_by(id Int, val Int) engine = Memory;

insert into test.limit_by values(1, 100), (1, 110), (1, 120), (1, 130), (2, 200), (2, 210), (2, 220), (3, 300);

select * from test.limit_by order by id, val limit 2, 2 by id;
select * from test.limit_by order by id, val limit 2 offset 1 by id;
select * from test.limit_by order by id, val limit 1, 2 by id limit 3;
select * from test.limit_by order by id, val limit 1, 2 by id limit 3 offset 1;
