drop table if exists ttl_00933_2;

create table ttl_00933_2 (d DateTime, a Int default 111 ttl d + interval 1 DAY) engine = MergeTree order by tuple() partition by toDayOfMonth(d);
insert into ttl_00933_2 values (toDateTime('2000-10-10 00:00:00'), 1);
insert into ttl_00933_2 values (toDateTime('2000-10-10 00:00:00'), 2);
insert into ttl_00933_2 values (toDateTime('2100-10-10 00:00:00'), 3);
insert into ttl_00933_2 values (toDateTime('2100-10-10 00:00:00'), 4);
select sleep(0.7) format Null; -- wait if very fast merge happen
optimize table ttl_00933_2 final;
select a from ttl_00933_2 order by a;

drop table if exists ttl_00933_2;

create table ttl_00933_2 (d DateTime, a Int, b default a * 2 ttl d + interval 1 DAY) engine = MergeTree order by tuple() partition by toDayOfMonth(d);
insert into ttl_00933_2 values (toDateTime('2000-10-10 00:00:00'), 1, 100);
insert into ttl_00933_2 values (toDateTime('2000-10-10 00:00:00'), 2, 200);
insert into ttl_00933_2 values (toDateTime('2100-10-10 00:00:00'), 3, 300);
insert into ttl_00933_2 values (toDateTime('2100-10-10 00:00:00'), 4, 400);
select sleep(0.7) format Null; -- wait if very fast merge happen
optimize table ttl_00933_2 final;
select a, b from ttl_00933_2 order by a;

drop table if exists ttl_00933_2;

create table ttl_00933_2 (d DateTime, a Int, b default 222 ttl d + interval 1 DAY) engine = MergeTree order by tuple() partition by toDayOfMonth(d);
insert into ttl_00933_2 values (toDateTime('2000-10-10 00:00:00'), 1, 5);
insert into ttl_00933_2 values (toDateTime('2000-10-10 00:00:00'), 2, 10);
insert into ttl_00933_2 values (toDateTime('2100-10-10 00:00:00'), 3, 15);
insert into ttl_00933_2 values (toDateTime('2100-10-10 00:00:00'), 4, 20);
select sleep(0.7) format Null; -- wait if very fast merge happen
optimize table ttl_00933_2 final;
select a, b from ttl_00933_2 order by a;

drop table if exists ttl_00933_2;
