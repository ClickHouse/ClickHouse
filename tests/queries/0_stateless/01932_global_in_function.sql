-- Tags: global

select number from cluster(test_cluster_two_shards_localhost, numbers(1)) where number global in tuple(0, 1, 2, 3);
select number from cluster(test_cluster_two_shards_localhost, numbers(1)) where number global in array(0, 1, 2, 3);
