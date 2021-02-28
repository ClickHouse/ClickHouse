drop table if exists local_table;
drop table if exists dist;

create table local_table (p Int64, t Int64, f Float64) Engine=MergeTree partition by p order by t settings index_granularity=1;

create table dist as local_table engine Distributed(test_cluster_two_shards_localhost, currentDatabase(), local_table) index hint p = 0;

insert into local_table select number / 4, number, number % 2 from numbers(16);

explain syntax select * from dist;
select * from dist;

drop table local_table;
drop table dist;
