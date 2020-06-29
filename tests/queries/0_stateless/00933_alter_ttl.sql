set send_logs_level = 'fatal';

drop table if exists ttl;

create table ttl (d Date, a Int) engine = MergeTree order by a partition by toDayOfMonth(d);
alter table ttl modify ttl d + interval 1 day;
show create table ttl;
insert into ttl values (toDateTime('2000-10-10 00:00:00'), 1);
insert into ttl values (toDateTime('2000-10-10 00:00:00'), 2);
insert into ttl values (toDateTime('2100-10-10 00:00:00'), 3);
insert into ttl values (toDateTime('2100-10-10 00:00:00'), 4);
select sleep(1) format Null; -- wait if very fast merge happen
optimize table ttl partition 10 final;

select * from ttl order by d;

alter table ttl modify ttl a; -- { serverError 450 }

drop table if exists ttl;

create table ttl (d Date, a Int) engine = MergeTree order by tuple() partition by toDayOfMonth(d);
alter table ttl modify column a Int ttl d + interval 1 day;
desc table ttl;
alter table ttl modify column d Int ttl d + interval 1 day; -- { serverError 43 }
alter table ttl modify column d DateTime ttl d + interval 1 day; -- { serverError 524 }

drop table if exists ttl;
