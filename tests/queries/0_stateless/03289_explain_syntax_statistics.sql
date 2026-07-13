create table data_01247 as system.numbers engine=Memory();
insert into data_01247 select * from system.numbers limit 2;
create table dist_01247 as data_01247 engine=Distributed(test_cluster_two_shards, currentDatabase(), data_01247, number);

set use_statistics = 1;
set ignore_format_null_for_explain = 0;

EXPLAIN SYNTAX SELECT 'Get hierarchy', toNullable(13), count() IGNORE NULLS FROM dist_01247 GROUP BY number WITH CUBE SETTINGS distributed_group_by_no_merge = 3 FORMAT Null;
