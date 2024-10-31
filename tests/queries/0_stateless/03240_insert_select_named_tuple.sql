SET enable_analyzer = 1;
SET enable_named_columns_in_function_tuple = 1;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;

CREATE TABLE src (id UInt32, type String, data String) ENGINE=MergeTree ORDER BY tuple();
CREATE TABLE dst (id UInt32, a Tuple (col_a Nullable(String), type String), b Tuple (col_b Nullable(String), type String)) ENGINE = MergeTree ORDER BY id;

INSERT INTO src VALUES (1, 'ok', 'data');
INSERT INTO dst (id, a, b) SELECT id, tuple(replaceAll(data, 'a', 'e') AS col_a, type) AS a, tuple(replaceAll(data, 'a', 'e') AS col_b, type) AS b FROM src;
SELECT * FROM dst;

DROP TABLE src;
DROP TABLE dst;

DROP TABLE IF EXISTS src;
CREATE TABLE src (id UInt32, type String, data String) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO src VALUES (1, 'ok', 'data');
SELECT id, tuple(replaceAll(data, 'a', 'e') AS col_a, type) AS a, tuple(replaceAll(data, 'a', 'e') AS col_b, type) AS b FROM cluster(test_cluster_two_shards, currentDatabase(), src) SETTINGS prefer_localhost_replica=0 FORMAT JSONEachRow;

DROP TABLE src;
