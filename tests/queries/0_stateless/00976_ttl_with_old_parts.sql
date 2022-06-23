drop table if exists ttl;

create table ttl (d Date, a Int) engine = MergeTree order by a partition by toDayOfMonth(d) settings remove_empty_parts = 0;
insert into ttl values (toDateTime('2000-10-10 00:00:00'), 1);
insert into ttl values (toDateTime('2000-10-10 00:00:00'), 2);
insert into ttl values (toDateTime('2100-10-10 00:00:00'), 3);
insert into ttl values (toDateTime('2100-10-10 00:00:00'), 4);

alter table ttl modify ttl d + interval 1 day;

select sleep(1) format Null; -- wait if very fast merge happen
optimize table ttl partition 10 final;

select * from ttl order by d;

drop table if exists ttl;
