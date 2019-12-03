SET use_experimental_local_query_cache = 1;

DROP TABLE IF EXISTS query_cache;
CREATE TABLE query_cache (dt Date, id int, name String) ENGINE = MergeTree() PARTITION BY dt ORDER BY id;

SELECT count() from query_cache where id in (SELECT id from query_cache WHERE dt = '2019-11-11' GROUP BY id); -- 0
SELECT count() from query_cache where name in (SELECT name from query_cache WHERE dt = '2019-11-11' GROUP BY name); -- 0

INSERT INTO query_cache values ('2019-11-11', 1, 'a');
SELECT count() from query_cache where id in (SELECT id from query_cache WHERE dt = '2019-11-11' GROUP BY id); -- 1
SELECT count() from query_cache where name in (SELECT name from query_cache WHERE dt = '2019-11-11' GROUP BY name); -- 1

INSERT INTO query_cache values ('2019-11-11', 2, 'b');
SELECT count() from query_cache where id in (SELECT id from query_cache WHERE dt = '2019-11-11' GROUP BY id); -- 2
SELECT count() from query_cache where name in (SELECT name from query_cache WHERE dt = '2019-11-11' GROUP BY name); -- 2

DROP TABLE query_cache;
CREATE TABLE query_cache (dt Date, id int, name String) ENGINE = MergeTree() PARTITION BY dt ORDER BY id;

SELECT count() from query_cache where id in (SELECT id from query_cache WHERE dt = '2019-11-11' GROUP BY id); -- 0
SELECT count() from query_cache where name in (SELECT name from query_cache WHERE dt = '2019-11-11' GROUP BY name); -- 0

INSERT INTO query_cache values ('2019-11-11', 1, 'a');
SELECT count() from query_cache where id in (SELECT id from query_cache WHERE dt = '2019-11-11' GROUP BY id); -- 1
SELECT count() from query_cache where name in (SELECT name from query_cache WHERE dt = '2019-11-11' GROUP BY name); -- 1

DROP QUERY_CACHE;
SELECT count() from query_cache where id in (SELECT id from query_cache WHERE dt = '2019-11-11' GROUP BY id); -- 1
SELECT count() from query_cache where name in (SELECT name from query_cache WHERE dt = '2019-11-11' GROUP BY name); -- 1

