drop table if exists ttl_01280_1;

create table ttl_01280_1 (a Int, b Int, x Int, y Int, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second delete where x % 10 == 0 and y > 5;
insert into ttl_01280_1 values (1, 1, 0, 4, now() + 10);
insert into ttl_01280_1 values (1, 1, 10, 6, now());
insert into ttl_01280_1 values (1, 2, 3, 7, now());
insert into ttl_01280_1 values (1, 3, 0, 5, now());
insert into ttl_01280_1 values (2, 1, 20, 1, now());
insert into ttl_01280_1 values (2, 1, 0, 1, now());
insert into ttl_01280_1 values (3, 1, 0, 8, now());
select sleep(1.1) format Null;
optimize table ttl_01280_1 final;
select a, b, x, y from ttl_01280_1;

drop table if exists ttl_01280_2;

create table ttl_01280_2 (a Int, b Int, x Array(Int32), y Double, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by a, b set x = minForEach(x), y = sum(y), d = max(d);
insert into ttl_01280_2 values (1, 1, array(0, 2, 3), 4, now() + 10);
insert into ttl_01280_2 values (1, 1, array(5, 4, 3), 6, now());
insert into ttl_01280_2 values (1, 1, array(5, 5, 1), 7, now());
insert into ttl_01280_2 values (1, 3, array(3, 0, 4), 5, now());
insert into ttl_01280_2 values (1, 3, array(1, 1, 2, 1), 9, now());
insert into ttl_01280_2 values (1, 3, array(3, 2, 1, 0), 3, now());
insert into ttl_01280_2 values (2, 1, array(3, 3, 3), 7, now());
insert into ttl_01280_2 values (2, 1, array(11, 1, 0, 3), 1, now());
insert into ttl_01280_2 values (3, 1, array(2, 4, 5), 8, now());
select sleep(1.1) format Null;
optimize table ttl_01280_2 final;
select a, b, x, y from ttl_01280_2;

drop table if exists ttl_01280_3;

create table ttl_01280_3 (a Int, b Int, x Int64, y Int, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by a set x = argMax(x, d), y = argMax(y, d), d = max(d);
insert into ttl_01280_3 values (1, 1, 0, 4, now() + 10);
insert into ttl_01280_3 values (1, 1, 10, 6, now() + 1);
insert into ttl_01280_3 values (1, 2, 3, 7, now());
insert into ttl_01280_3 values (1, 3, 0, 5, now());
insert into ttl_01280_3 values (2, 1, 20, 1, now());
insert into ttl_01280_3 values (2, 1, 0, 3, now() + 1);
insert into ttl_01280_3 values (3, 1, 0, 3, now());
insert into ttl_01280_3 values (3, 2, 8, 2, now() + 1);
insert into ttl_01280_3 values (3, 5, 5, 8, now());
select sleep(2.1) format Null;
optimize table ttl_01280_3 final;
select a, b, x, y from ttl_01280_3;