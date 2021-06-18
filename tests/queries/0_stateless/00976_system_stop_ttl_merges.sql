drop table if exists ttl;

create table ttl (d Date, a Int) engine = MergeTree order by a partition by toDayOfMonth(d) ttl d + interval 1 day;

system stop ttl merges;

insert into ttl values (toDateTime('2000-10-10 00:00:00'), 1), (toDateTime('2000-10-10 00:00:00'), 2)
insert into ttl values (toDateTime('2100-10-10 00:00:00'), 3), (toDateTime('2100-10-10 00:00:00'), 4);

select sleep(1) format Null; -- wait if very fast merge happen
optimize table ttl partition 10 final;
select * from ttl order by d, a;

system start ttl merges;
optimize table ttl partition 10 final;
select * from ttl order by d, a;

drop table if exists ttl;
