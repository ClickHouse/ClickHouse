drop table if exists ttl_01246_1;

create table ttl_01246_1 (a Int, b Int, x Int, y Int, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second delete where x % 10 == 0 and y > 5;
insert into ttl_01246_1 values (1, 1, 0, 4, now() + 10);
insert into ttl_01246_1 values (1, 1, 10, 6, now());
insert into ttl_01246_1 values (1, 2, 3, 7, now());
insert into ttl_01246_1 values (1, 3, 0, 5, now());
insert into ttl_01246_1 values (2, 1, 20, 1, now());
insert into ttl_01246_1 values (2, 1, 0, 1, now());
insert into ttl_01246_1 values (3, 1, 0, 8, now());
select sleep(1.1) format Null;
optimize table ttl_01246_1 final;
select a, b, x, y from ttl_01246_1;

drop table if exists ttl_01246_1;

create table ttl_01246_1 (a Int, b Int, x Int32, y Double, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by a, b set x = cast(median(x) as Int32), y = avg(y), d = max(d);
insert into ttl_01246_1 values (1, 1, 0, 4, now() + 10);
insert into ttl_01246_1 values (1, 1, 10, 6, now());
insert into ttl_01246_1 values (1, 2, 3, 7, now());
insert into ttl_01246_1 values (1, 3, 0, 5, now());
insert into ttl_01246_1 values (2, 1, 20, 1, now());
insert into ttl_01246_1 values (2, 1, 0, 1, now());
insert into ttl_01246_1 values (3, 1, 0, 8, now());
select sleep(1.1) format Null;
optimize table ttl_01246_1 final;
select a, b, x, y from ttl_01246_1;

drop table if exists ttl_01246_1;

create table ttl_01246_1 (a Int, b Int, x Int32, y Int, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by a set x = max(x), y = cast(round(avg(y)) as Int), d = max(d);
insert into ttl_01246_1 values (1, 1, 0, 4, now() + 10);
insert into ttl_01246_1 values (1, 1, 10, 6, now());
insert into ttl_01246_1 values (1, 2, 3, 7, now());
insert into ttl_01246_1 values (1, 3, 0, 5, now());
insert into ttl_01246_1 values (2, 1, 20, 1, now());
insert into ttl_01246_1 values (2, 1, 0, 1, now());
insert into ttl_01246_1 values (3, 1, 0, 8, now());
select sleep(1.1) format Null;
optimize table ttl_01246_1 final;
select a, b, x, y from ttl_01246_1;