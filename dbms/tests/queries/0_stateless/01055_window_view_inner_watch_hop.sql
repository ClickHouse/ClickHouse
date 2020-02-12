SET allow_experimental_window_view = 1;

DROP TABLE IF EXISTS test.wv;
DROP TABLE IF EXISTS test.mt;

CREATE TABLE test.mt(a Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW test.wv ENGINE=MergeTree ORDER BY tuple() AS SELECT count(a) FROM test.mt GROUP BY HOP(timestamp, INTERVAL '1' SECOND, INTERVAL '2' SECOND) AS wid;

INSERT INTO test.mt VALUES (1, now());
WATCH test.wv LIMIT 1;

DROP TABLE test.wv;
DROP TABLE test.mt;
