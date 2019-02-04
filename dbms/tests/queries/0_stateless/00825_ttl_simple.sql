drop table if exists test.ttl;

create table test.ttl (d DateTime, a Int ttl d + interval 1 second, b Int ttl d + interval 1 second) engine = MergeTree order by a partition by toMinute(d);
insert into test.ttl values (now(), 1, 2);
insert into test.ttl values (now(), 3, 4);
select sleep(1.1);
optimize table test.ttl;
select a, b from test.ttl;

drop table if exists test.ttl;

create table test.ttl (d DateTime ttl d + interval 1 second, a Int ttl d + interval 1 second) engine = MergeTree order by a partition by toMinute(d);
insert into test.ttl values (now(), 1);
insert into test.ttl values (now(), 3);
select sleep(1.1);
optimize table test.ttl;
select * from test.ttl;

drop table if exists test.ttl;

create table test.ttl (d DateTime, a Int ttl d + interval 1 DAY) engine = MergeTree order by a partition by toDayOfMonth(d);
insert into test.ttl values (toDateTime('2000-10-10 00:00:00'), 1);
insert into test.ttl values (toDateTime('2000-10-11 00:00:00'), 2);
insert into test.ttl values (toDateTime('2000-10-12 00:00:00'), 3);
select sleep(0.1);
select * from test.ttl order by d;

SET send_logs_level = 'none';

drop table if exists test.ttl;
create table test.ttl (d DateTime ttl 2 + 2) engine = MergeTree order by tuple() partition by toSecond(d); -- { serverError 440}
create table test.ttl (d DateTime ttl toDateTime(1)) engine = MergeTree order by tuple() partition by toSecond(d); -- { serverError 440}
create table test.ttl (d DateTime ttl d - d) engine = MergeTree order by tuple() partition by toSecond(d); -- { serverError 440}
