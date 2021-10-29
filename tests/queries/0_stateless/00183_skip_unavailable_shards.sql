-- Tags: shard

SET send_logs_level = 'fatal';
SELECT count() FROM remote('{127,1}.0.0.{2,3}', system.one) SETTINGS skip_unavailable_shards = 1;
SELECT count() FROM remote('{1,127}.0.0.{2,3}', system.one) SETTINGS skip_unavailable_shards = 1;
