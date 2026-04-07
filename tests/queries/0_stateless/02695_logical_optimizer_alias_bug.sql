create table test_local (id UInt32, path LowCardinality(String)) engine = MergeTree order by id;
WITH ((position(path, '/a') > 0) AND (NOT (position(path, 'a') > 0))) OR (path = '/b') OR (path = '/b/') as alias1 SELECT max(alias1) FROM remote('127.0.0.{1,2}', currentDatabase(), test_local) WHERE (id = 299386662);
