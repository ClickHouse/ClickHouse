drop table if exists ttl;
set mutations_sync = 2;

-- check that ttl info was updated after mutation.
create table ttl (i Int, a Int, s String) engine = MergeTree order by i;
insert into ttl values (1, 1, 'a') (2, 1, 'b') (3, 1, 'c') (4, 1, 'd');

alter table ttl modify ttl a % 2 = 0 ? today() - 10 : toDate('2100-01-01');
alter table ttl materialize ttl;

select * from ttl order by i;
alter table ttl update a = 0 where i % 2 = 0;
select * from ttl order by i;

drop table ttl;

select '===================';

-- check that skip index is updated after column was modified by ttl.
create table ttl (i Int, a Int, s String default 'b' ttl a % 2 = 0 ? today() - 10 : toDate('2100-01-01'),
    index ind_s (s) type set(1) granularity 1) engine = MergeTree order by i;
insert into ttl values (1, 1, 'a') (2, 1, 'a') (3, 1, 'a') (4, 1, 'a');

select count() from ttl where s = 'a';

alter table ttl update a = 0 where i % 2 = 0;

select count() from ttl where s = 'a';
select count() from ttl where s = 'b';

drop table ttl;

-- check only that it doesn't throw exceptions.
create table ttl (i Int, s String) engine = MergeTree order by i ttl toDate('2000-01-01') TO DISK 'default';
alter table ttl materialize ttl;
drop table ttl;

create table ttl (a Int, b Int, c Int default 42 ttl d, d Date, index ind (b * c) type minmax granularity 1)
engine = MergeTree order by a;
insert into ttl values (1, 2, 3, '2100-01-01');
alter table ttl update d = '2000-01-01' where 1;
alter table ttl materialize ttl;
select * from ttl;
drop table ttl;
