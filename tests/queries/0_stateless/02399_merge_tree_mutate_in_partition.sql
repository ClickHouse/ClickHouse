
drop table if exists mt;
drop table if exists m;

create table mt (p int, n int) engine=MergeTree order by tuple() partition by p;
create table m (n int) engine=Memory;
insert into mt values (1, 1), (2, 1);
insert into mt values (1, 2), (2, 2);
select *, _part from mt order by _part;

alter table  mt update n = n + (n not in m) in partition id '1' where 1 settings mutations_sync=1;
drop table m;
optimize table mt final;

select mutation_id, command, parts_to_do_names, parts_to_do, is_done from system.mutations where database=currentDatabase();
select * from mt order by p, n;

drop table mt;
