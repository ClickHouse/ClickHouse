drop table if exists test.ttl;

create table test.ttl (d DateTime, a Int default 111 ttl d + interval 1 DAY) engine = MergeTree order by tuple() partition by toDayOfMonth(d);
insert into test.ttl values (toDateTime('2000-10-10 00:00:00'), 1);
insert into test.ttl values (toDateTime('2000-10-10 00:00:00'), 2);
optimize table test.ttl final;
select a from test.ttl order by d;

drop table if exists test.ttl;

create table test.ttl (d DateTime, a Int, b default a * 2 ttl d + interval 1 DAY) engine = MergeTree order by tuple() partition by toDayOfMonth(d);
insert into test.ttl values (toDateTime('2000-10-10 00:00:00'), 1, 100);
insert into test.ttl values (toDateTime('2000-10-10 00:00:00'), 2, 200);
optimize table test.ttl final;
select a, b from test.ttl order by d;