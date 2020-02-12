SET allow_experimental_window_view = 1;

DROP TABLE IF EXISTS test.wv;
DROP TABLE IF EXISTS test.mt;
DROP TABLE IF EXISTS test.dst;
DROP TABLE IF EXISTS test.`.inner.wv`;

CREATE TABLE test.mt(a Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();
CREATE TABLE test.dst(count UInt64) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW test.wv TO test.dst ENGINE=MergeTree ORDER BY tuple() AS SELECT count(a) AS count FROM test.mt GROUP BY TUMBLE(timestamp, INTERVAL '1' SECOND) AS wid;

INSERT INTO test.mt VALUES (1, now());
SELECT sleep(1);
SELECT count FROM test.dst;

DROP TABLE test.wv;
DROP TABLE test.mt;
DROP TABLE test.dst;