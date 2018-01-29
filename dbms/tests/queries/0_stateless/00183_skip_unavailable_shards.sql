SELECT count() FROM remote('{127,1}.0.0.{2,3}', system.one) SETTINGS skip_unavailable_shards = 1;
