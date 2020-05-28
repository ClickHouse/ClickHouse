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

drop table if exists ttl_01280_4;

create table ttl_01280_4 (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (toDate(d), -(a + b)) ttl d + interval 1 second group by toDate(d) set x = sum(x), y = max(y);
insert into ttl_01280_4 values (1, 1, 0, 4, now() + 10);
insert into ttl_01280_4 values (10, 2, 3, 3, now());
insert into ttl_01280_4 values (2, 10, 1, 7, now());
insert into ttl_01280_4 values (3, 3, 5, 2, now());
insert into ttl_01280_4 values (1, 5, 4, 9, now());
select sleep(1.1) format Null;
optimize table ttl_01280_4 final;
select a, b, x, y from ttl_01280_4;

drop table if exists ttl_01280_5;

create table ttl_01280_5 (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (toDate(d), a, -b) ttl d + interval 1 second group by toDate(d), a set x = sum(x);
insert into ttl_01280_5 values (1, 2, 3, 5, now());
insert into ttl_01280_5 values (2, 10, 1, 5, now());
insert into ttl_01280_5 values (2, 3, 5, 5, now());
insert into ttl_01280_5 values (1, 5, 4, 5, now());
select sleep(1.1) format Null;
optimize table ttl_01280_5 final;
select a, b, x, y from ttl_01280_5;

drop table if exists ttl_01280_6;

create table ttl_01280_6 (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (toDate(d), a, -b) ttl d + interval 1 second group by toDate(d), a;
insert into ttl_01280_6 values (1, 2, 3, 5, now());
insert into ttl_01280_6 values (2, 10, 3, 5, now());
insert into ttl_01280_6 values (2, 3, 3, 5, now());
insert into ttl_01280_6 values (1, 5, 3, 5, now());
select sleep(1.1) format Null;
optimize table ttl_01280_6 final;
select a, b, x, y from ttl_01280_6;

create table ttl_01280_error (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by x set y = max(y); -- { serverError 450}
create table ttl_01280_error (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by b set y = max(y); -- { serverError 450}
create table ttl_01280_error (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by a, b, x set y = max(y); -- { serverError 450}
create table ttl_01280_error (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by a set b = min(b), y = max(y); -- { serverError 450}
create table ttl_01280_error (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by a, b set y = max(y), y = max(y); -- { serverError 450}
create table ttl_01280_error (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (toDate(d), a) ttl d + interval 1 second group by toDate(d), a set d = min(d), b = max(b); -- { serverError 450}
create table ttl_01280_error (a Int, b Int, x Int64, y Int64, d DateTime) engine = MergeTree order by (d, -(a + b)) ttl d + interval 1 second group by d, -(a + b) set a = sum(a), b = min(b); -- { serverError 450}
