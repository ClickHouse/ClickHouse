create table dist_01756 (dummy UInt8) ENGINE = Distributed('test_cluster_two_shards', 'system', 'one', dummy);
select ignore(1), * from dist_01756 where 0 settings optimize_skip_unused_shards=1, force_optimize_skip_unused_shards=1;
drop table dist_01756;
