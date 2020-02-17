SET allow_experimental_window_view = 1;

DROP TABLE IF EXISTS test.mt;
DROP TABLE IF EXISTS test.dst;
DROP TABLE IF EXISTS test.`.inner.wv`;

CREATE TABLE test.dst(count UInt64) Engine=MergeTree ORDER BY tuple();

SELECT '--TUMBLE--';
DROP TABLE IF EXISTS test.wv;
TRUNCATE TABLE test.dst;
CREATE TABLE test.mt(a Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW test.wv TO test.dst WATERMARK INTERVAL '1' SECOND AS SELECT count(a) AS count FROM test.mt GROUP BY TUMBLE(timestamp, INTERVAL '1' SECOND) AS wid;

INSERT INTO test.mt VALUES (1, now());
SELECT sleep(2);
SELECT count FROM test.dst;

SELECT '--HOP--';
DROP TABLE IF EXISTS test.wv;
TRUNCATE TABLE test.dst;
CREATE WINDOW VIEW test.wv TO test.dst WATERMARK INTERVAL '1' SECOND AS SELECT count(a) AS count FROM test.mt GROUP BY HOP(timestamp, INTERVAL '1' SECOND, INTERVAL '2' SECOND) AS wid;

INSERT INTO test.mt VALUES (1, now());
SELECT sleep(2);
SELECT count FROM test.dst;

SELECT '--INNER_TUMBLE--';
DROP TABLE IF EXISTS test.wv;
TRUNCATE TABLE test.dst;
CREATE WINDOW VIEW test.wv TO test.dst ENGINE=MergeTree ORDER BY tuple() WATERMARK INTERVAL '1' SECOND AS SELECT count(a) AS count FROM test.mt GROUP BY TUMBLE(timestamp, INTERVAL '1' SECOND) AS wid;

INSERT INTO test.mt VALUES (1, now());
SELECT sleep(2);
SELECT count FROM test.dst;

SELECT '--INNER_HOP--';
DROP TABLE IF EXISTS test.wv;
TRUNCATE TABLE test.dst;
CREATE WINDOW VIEW test.wv TO test.dst ENGINE=MergeTree ORDER BY tuple() WATERMARK INTERVAL '1' SECOND AS SELECT count(a) AS count FROM test.mt GROUP BY HOP(timestamp, INTERVAL '1' SECOND, INTERVAL '2' SECOND) AS wid;

INSERT INTO test.mt VALUES (1, now());
SELECT sleep(2);
SELECT count FROM test.dst;

DROP TABLE test.wv;
DROP TABLE test.mt;
