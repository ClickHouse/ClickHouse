SET allow_experimental_window_view = 1;

DROP TABLE IF EXISTS test.wv;
DROP TABLE IF EXISTS test.mt;

CREATE TABLE test.mt(a Int32, timestamp DateTime) Engine=MergeTree order by tuple();
CREATE WINDOW VIEW test.wv AS SELECT count(a), HOP_START(wid) as w_start, HOP_END(wid) as w_end FROM test.mt group by HOP(timestamp, INTERVAL '1' SECOND, INTERVAL '5' SECOND) as wid order by w_start;

INSERT INTO test.mt VALUES (1, toDateTime('1990-01-01 12:00:01'));
WATCH test.wv LIMIT 1;

INSERT INTO test.mt VALUES (3, toDateTime('1990-01-01 12:00:10'));
WATCH test.wv LIMIT 1;

DROP TABLE test.wv;
DROP TABLE test.mt;
