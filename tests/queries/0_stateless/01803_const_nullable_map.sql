DROP TABLE IF EXISTS t_map_null;

CREATE TABLE t_map_null (a Map(String, String), b String) engine = MergeTree() ORDER BY a;
INSERT INTO t_map_null VALUES (map('a', 'b', 'c', 'd'), 'foo');
SELECT count() FROM t_map_null WHERE a = map('name', NULL, '', NULL);

DROP TABLE t_map_null;
