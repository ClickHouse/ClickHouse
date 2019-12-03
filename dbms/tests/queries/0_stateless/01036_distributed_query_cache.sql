SET use_experimental_local_query_cache = 1;
SET use_experimental_distributed_query_cache = 1;

DROP TABLE IF EXISTS query_cache;
CREATE TABLE query_cache (dt Date, id int, name String) ENGINE = MergeTree() PARTITION BY dt ORDER BY id;

DROP TABLE IF EXISTS distributed_query_cache;
CREATE TABLE distributed_query_cache (dt Date, id int, name String) ENGINE = Distributed('test_cluster_two_shards', default, query_cache);

SELECT count() from distributed_query_cache where id in (SELECT id from query_cache WHERE dt = '2019-11-11' GROUP BY id); -- 0
DROP QUERY_CACHE;

INSERT INTO query_cache values ('2019-11-11', 1, 'a');
SELECT count() from distributed_query_cache where id in (SELECT id from query_cache WHERE dt = '2019-11-11' GROUP BY id); -- 2
SELECT count() from distributed_query_cache where id in (SELECT id from query_cache WHERE dt = '2019-11-11' GROUP BY id); -- 2
