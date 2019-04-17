drop table if exists test.ttl;

create table test.ttl (d DateTime, a Int ttl d + interval 1 second, b Int ttl d + interval 1 second) engine = MergeTree order by tuple() partition by toMinute(d);
insert into test.ttl values (now(), 1, 2);
insert into test.ttl values (now(), 3, 4);
select sleep(1.1) format Null;
optimize table test.ttl final;
select a, b from test.ttl;

drop table if exists test.ttl;

create table test.ttl (d DateTime, a Int ttl d + interval 1 DAY) engine = MergeTree order by tuple() partition by toDayOfMonth(d);
insert into test.ttl values (toDateTime('2000-10-10 00:00:00'), 1);
insert into test.ttl values (toDateTime('2000-10-10 00:00:00'), 2);
insert into test.ttl values (toDateTime('2000-10-10 00:00:00'), 3);
select sleep(0.7) format Null; -- wait if very fast merge happen
optimize table test.ttl final;
select * from test.ttl order by d;

drop table if exists test.ttl;

create table test.ttl (d DateTime, a Int) engine = MergeTree order by tuple() partition by tuple() ttl d + interval 1 day;
insert into test.ttl values (toDateTime('2000-10-10 00:00:00'), 1);
insert into test.ttl values (toDateTime('2000-10-10 00:00:00'), 2);
insert into test.ttl values (toDateTime('2100-10-10 00:00:00'), 3);
select sleep(0.7) format Null; -- wait if very fast merge happen
optimize table test.ttl final;
select * from test.ttl order by d;

drop table if exists test.ttl;

create table test.ttl (d Date, a Int) engine = MergeTree order by a partition by toDayOfMonth(d) ttl d + interval 1 day;
insert into test.ttl values (toDate('2000-10-10'), 1);
insert into test.ttl values (toDate('2100-10-10'), 2);
select sleep(0.7) format Null; -- wait if very fast merge happen
optimize table test.ttl final;
select * from test.ttl order by d;

set send_logs_level = 'none';

drop table if exists test.ttl;

create table test.ttl (d DateTime ttl d) engine = MergeTree order by tuple() partition by toSecond(d); -- { serverError 44}
create table test.ttl (d DateTime, a Int ttl d) engine = MergeTree order by a partition by toSecond(d); -- { serverError 44}
create table test.ttl (d DateTime, a Int ttl 2 + 2) engine = MergeTree order by tuple() partition by toSecond(d); -- { serverError 450 }
create table test.ttl (d DateTime, a Int ttl toDateTime(1)) engine = MergeTree order by tuple() partition by toSecond(d); -- { serverError 450 }
create table test.ttl (d DateTime, a Int ttl d - d) engine = MergeTree order by tuple() partition by toSecond(d); -- { serverError 450 }
