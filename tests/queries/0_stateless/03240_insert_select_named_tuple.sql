DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;

CREATE TABLE src (id UInt32, type String, data String) ENGINE=MergeTree ORDER BY tuple();
CREATE TABLE dst (id UInt32, a Tuple (col_a Nullable(String), type String), b Tuple (col_b Nullable(String), type String)) ENGINE = MergeTree ORDER BY id;

INSERT INTO src VALUES (1, 'ok', 'data');
INSERT INTO dst (id, a, b) SELECT id, tuple(replaceAll(data, 'a', 'e') AS col_a, type) AS a, tuple(replaceAll(data, 'a', 'e') AS col_b, type) AS b FROM src;
SELECT * FROM dst;

DROP TABLE src;
DROP TABLE dst;
