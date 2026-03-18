-- Tags: no-parallel, no-random-merge-tree-settings

drop table if exists test;

create table test (i int) engine MergeTree order by tuple();

insert into test select arrayJoin(range(10000));

set exact_rows_before_limit = 1, output_format_write_statistics = 0, max_block_size = 100;

select * from test limit 1 FORMAT JSONCompact;

select * from test where i < 10 group by i order by i limit 1 FORMAT JSONCompact;

select * from test group by i having i in (10, 11, 12) order by i limit 1 FORMAT JSONCompact;

select * from test where i < 20 order by i limit 1 FORMAT JSONCompact;

set prefer_localhost_replica = 0;
select * from cluster(test_cluster_two_shards, currentDatabase(), test) where i < 30 order by i limit 1 FORMAT JSONCompact;
select * from cluster(test_cluster_two_shards, currentDatabase(), test) where i < 20 order by i limit 1 FORMAT JSONCompact;

set prefer_localhost_replica = 1;
select * from cluster(test_cluster_two_shards, currentDatabase(), test) where i < 30 order by i limit 1 FORMAT JSONCompact;
select * from cluster(test_cluster_two_shards, currentDatabase(), test) where i < 20 order by i limit 1 FORMAT JSONCompact;

select * from (select * from cluster(test_cluster_two_shards, currentDatabase(), test) where i < 10) order by i limit 1 FORMAT JSONCompact;

drop table if exists test;
