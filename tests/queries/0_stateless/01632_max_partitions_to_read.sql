drop table if exists p;

create table p(d Date, i int, j int) engine MergeTree partition by d order by i settings max_partitions_to_read = 1;

insert into p values ('2021-01-01', 1, 2), ('2021-01-02', 4, 5);

select * from p order by i; -- { serverError TOO_MANY_PARTITIONS }

select * from p order by i settings max_partitions_to_read = 2;

select * from p order by i settings max_partitions_to_read = 0; -- unlimited

alter table p modify setting max_partitions_to_read = 2;

select * from p order by i;

drop table if exists p;
