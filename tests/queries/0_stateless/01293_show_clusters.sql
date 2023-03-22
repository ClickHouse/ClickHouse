-- don't show all clusters to reduce dependency on the configuration of server
set send_logs_level = 'fatal';
show clusters like 'test_shard%' limit 1;
show cluster 'test_shard_localhost';
