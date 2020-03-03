SET allow_experimental_window_view = 1;
SET allow_experimental_window_view = 1;

DROP TABLE IF EXISTS test.mt;
DROP TABLE IF EXISTS test.dst;
DROP TABLE IF EXISTS test.wv;

CREATE TABLE test.dst(count UInt64, w_end DateTime) Engine=MergeTree ORDER BY tuple();
CREATE TABLE test.mt(a Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();

CREATE WINDOW VIEW test.wv TO test.dst WATERMARK=ASCENDING ALLOWED_LATENESS=INTERVAL '2' SECOND AS SELECT count(a) AS count, TUMBLE_END(wid) AS w_end FROM test.mt GROUP BY TUMBLE(timestamp, INTERVAL '5' SECOND) AS wid;

INSERT INTO test.mt VALUES (1, '1990/01/01 12:00:00');
INSERT INTO test.mt VALUES (1, '1990/01/01 12:00:02');
INSERT INTO test.mt VALUES (1, '1990/01/01 12:00:05');
INSERT INTO test.mt VALUES (1, '1990/01/01 12:00:06');
INSERT INTO test.mt VALUES (1, '1990/01/01 12:00:05');
INSERT INTO test.mt VALUES (1, '1990/01/01 12:00:04');
INSERT INTO test.mt VALUES (1, '1990/01/01 12:00:03');
INSERT INTO test.mt VALUES (1, '1990/01/01 12:00:07');
INSERT INTO test.mt VALUES (1, '1990/01/01 12:00:10');
INSERT INTO test.mt VALUES (1, '1990/01/01 12:00:11');

SELECT sleep(1);
SELECT * from test.dst order by w_end, count;

DROP TABLE test.wv;
DROP TABLE test.mt;
DROP TABLE test.dst;
