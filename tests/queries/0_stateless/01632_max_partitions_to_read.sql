drop table if exists p;

create table p(d Date, i int, j int) engine MergeTree partition by d order by i settings max_partitions_to_read = 1;

insert into p values (yesterday(), 1, 2), (today(), 3, 4);

select * from p order by i; -- default no limit

select * from p order by i settings force_max_partition_limit = 0;

select * from p order by i settings force_max_partition_limit = 1; -- { serverError 565 }

alter table p modify setting max_partitions_to_read = 2;

select * from p order by i settings force_max_partition_limit = 1;

drop table if exists p;
