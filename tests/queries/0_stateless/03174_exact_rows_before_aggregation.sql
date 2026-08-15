-- Tags: no-parallel, no-random-merge-tree-settings

set rows_before_aggregation = 1, exact_rows_before_limit = 1, output_format_write_statistics = 0, max_block_size = 100;

drop table if exists test;

create table test (i int) engine MergeTree order by tuple();
insert into test select arrayJoin(range(10000));

select * from test where i < 10 group by i order by i FORMAT JSONCompact;
select * from test where i < 10 group by i order by i FORMAT XML;
select * from test group by i having i in (10, 11, 12) order by i FORMAT JSONCompact;
select * from test where i < 20 group by i order by i FORMAT JSONCompact;
select max(i) from test where i < 20 limit 1 FORMAT JSONCompact;

set prefer_localhost_replica = 0;
select * from cluster(test_cluster_two_shards, currentDatabase(), test) where i < 30 group by i order by i FORMAT JSONCompact;
select * from cluster(test_cluster_two_shards, currentDatabase(), test) where i < 20 group by i order by i FORMAT JSONCompact;

set prefer_localhost_replica = 1;
select * from cluster(test_cluster_two_shards, currentDatabase(), test) where i < 30 group by i order by i FORMAT JSONCompact;
select * from cluster(test_cluster_two_shards, currentDatabase(), test) where i < 20 group by i order by i FORMAT JSONCompact;

select max(i) from cluster(test_cluster_two_shards, currentDatabase(), test) where i < 20 FORMAT JSONCompact;

select * from (select * from cluster(test_cluster_two_shards, currentDatabase(), test) where i < 10) group by i order by i limit 10 FORMAT JSONCompact;
set prefer_localhost_replica = 0;
select * from (select * from cluster(test_cluster_two_shards, currentDatabase(), test) where i < 10) group by i order by i limit 10 FORMAT JSONCompact;

drop table if exists test;

create table test (i int) engine MergeTree order by i;

insert into test select arrayJoin(range(10000));

set optimize_aggregation_in_order=1;

select * from test where i < 10 group by i order by i FORMAT JSONCompact;
select max(i) from test where i < 20 limit 1 FORMAT JSONCompact;

set prefer_localhost_replica = 0;
select * from cluster(test_cluster_two_shards, currentDatabase(), test) where i < 30 group by i order by i FORMAT JSONCompact;

set prefer_localhost_replica = 1;
select * from cluster(test_cluster_two_shards, currentDatabase(), test) where i < 30 group by i order by i FORMAT JSONCompact;

drop table if exists test;
