drop table if exists ttl;
set mutations_sync = 2;
set materialize_ttl_after_modify = 0;

create table ttl (d Date, a Int) engine = MergeTree order by a partition by toDayOfMonth(d)
SETTINGS merge_with_ttl_timeout=0,max_number_of_merges_with_ttl_in_pool=0;

insert into ttl values (toDateTime('2000-10-10 00:00:00'), 1);
insert into ttl values (toDateTime('2000-10-10 00:00:00'), 2);
insert into ttl values (toDateTime('2100-10-10 00:00:00'), 3);
insert into ttl values (toDateTime('2100-10-10 00:00:00'), 4);

alter table ttl materialize ttl; -- { serverError 80 }

alter table ttl modify ttl d + interval 1 day;
-- TTL should not be applied
select * from ttl order by a;

alter table ttl materialize ttl;
select * from ttl order by a;
alter table ttl modify setting max_number_of_merges_with_ttl_in_pool = 1;
optimize table ttl;
select * from ttl order by a;

drop table if exists ttl;

create table ttl (i Int, s String) engine = MergeTree order by i
SETTINGS merge_with_ttl_timeout=0,max_number_of_merges_with_ttl_in_pool=0;
insert into ttl values (1, 'a') (2, 'b') (3, 'c') (4, 'd');

alter table ttl modify ttl i % 2 = 0 ? today() - 10 : toDate('2100-01-01');
alter table ttl materialize ttl;
select * from ttl order by i;
alter table ttl modify setting max_number_of_merges_with_ttl_in_pool = 1;
optimize table ttl;
select * from ttl order by i;
alter table ttl modify setting max_number_of_merges_with_ttl_in_pool = 0;

alter table ttl modify ttl toDate('2000-01-01');
alter table ttl materialize ttl;
select * from ttl order by i;
alter table ttl modify setting max_number_of_merges_with_ttl_in_pool = 1;
optimize table ttl;
select * from ttl order by i;

drop table if exists ttl;

create table ttl (i Int, s String) engine = MergeTree order by i
SETTINGS merge_with_ttl_timeout=0,max_number_of_merges_with_ttl_in_pool=0;
insert into ttl values (1, 'a') (2, 'b') (3, 'c') (4, 'd');

alter table ttl modify column s String ttl i % 2 = 0 ? today() - 10 : toDate('2100-01-01');
-- TTL should not be applied
select * from ttl order by i;

alter table ttl materialize ttl;
select * from ttl order by i;
alter table ttl modify setting max_number_of_merges_with_ttl_in_pool = 1;
optimize table ttl;
select * from ttl order by i;
alter table ttl modify setting max_number_of_merges_with_ttl_in_pool = 0;

alter table ttl modify column s String ttl toDate('2000-01-01');
alter table ttl materialize ttl;
select * from ttl order by i;
alter table ttl modify setting max_number_of_merges_with_ttl_in_pool = 1;
optimize table ttl;
select * from ttl order by i;

drop table if exists ttl;

create table ttl (d Date, i Int, s String) engine = MergeTree order by i
SETTINGS merge_with_ttl_timeout=0,max_number_of_merges_with_ttl_in_pool=0;

insert into ttl values (toDate('2000-01-02'), 1, 'a') (toDate('2000-01-03'), 2, 'b') (toDate('2080-01-01'), 3, 'c') (toDate('2080-01-03'), 4, 'd');

alter table ttl modify ttl i % 3 = 0 ? today() - 10 : toDate('2100-01-01');
alter table ttl materialize ttl;
select i, s from ttl order by i;
alter table ttl modify setting max_number_of_merges_with_ttl_in_pool = 1;
optimize table ttl;
select i, s from ttl order by i;
alter table ttl modify setting max_number_of_merges_with_ttl_in_pool = 0;

alter table ttl modify column s String ttl d + interval 1 month;
alter table ttl materialize ttl;
select i, s from ttl order by i;
alter table ttl modify setting max_number_of_merges_with_ttl_in_pool = 1;
optimize table ttl;
select i, s from ttl order by i;

drop table if exists ttl;
