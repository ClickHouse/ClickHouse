drop table if exists test.ttl;

create table test.ttl (d DateTime, a Int default 111 ttl d + interval 1 DAY) engine = MergeTree order by tuple() partition by toDayOfMonth(d);
insert into test.ttl values (toDateTime('2000-10-10 00:00:00'), 1);
insert into test.ttl values (toDateTime('2000-10-10 00:00:00'), 2);
insert into test.ttl values (toDateTime('2100-10-10 00:00:00'), 3);
insert into test.ttl values (toDateTime('2100-10-10 00:00:00'), 4);
optimize table test.ttl final;
select a from test.ttl order by a;

drop table if exists test.ttl;

create table test.ttl (d DateTime, a Int, b default a * 2 ttl d + interval 1 DAY) engine = MergeTree order by tuple() partition by toDayOfMonth(d);
insert into test.ttl values (toDateTime('2000-10-10 00:00:00'), 1, 100);
insert into test.ttl values (toDateTime('2000-10-10 00:00:00'), 2, 200);
insert into test.ttl values (toDateTime('2100-10-10 00:00:00'), 3, 300);
insert into test.ttl values (toDateTime('2100-10-10 00:00:00'), 4, 400);
optimize table test.ttl final;
select a, b from test.ttl order by a;

drop table if exists test.ttl;

create table test.ttl (d DateTime, a Int, b default 222 ttl d + interval 1 DAY) engine = MergeTree order by tuple() partition by toDayOfMonth(d);
insert into test.ttl values (toDateTime('2000-10-10 00:00:00'), 1, 5);
insert into test.ttl values (toDateTime('2000-10-10 00:00:00'), 2, 10);
insert into test.ttl values (toDateTime('2100-10-10 00:00:00'), 3, 15);
insert into test.ttl values (toDateTime('2100-10-10 00:00:00'), 4, 20);
optimize table test.ttl final;
select a, b from test.ttl order by a;

