set mutations_sync = 2;

drop table if exists ttl;

create table ttl (d Date, a Int) engine = MergeTree order by a partition by toDayOfMonth(d);
insert into ttl values (toDateTime('2000-10-10 00:00:00'), 1);
insert into ttl values (toDateTime('2000-10-10 00:00:00'), 2);
insert into ttl values (toDateTime('2100-10-10 00:00:00'), 3);
insert into ttl values (toDateTime('2100-10-10 00:00:00'), 4);

alter table ttl materialize ttl; -- { serverError 80 }

alter table ttl modify ttl d + interval 1 day;

alter table ttl materialize ttl;

select * from ttl order by a;

drop table if exists ttl;
