SET allow_experimental_window_view = 1;

DROP TABLE IF EXISTS test.wv;
DROP TABLE IF EXISTS test.mt;

CREATE TABLE test.mt(a Int32, timestamp DateTime) Engine=MergeTree order by tuple();
CREATE WINDOW VIEW test.wv AS SELECT count(a), TUMBLE_START(wid) as w_start, TUMBLE_END(wid) as w_end FROM test.mt group by TUMBLE(timestamp, INTERVAL '5' SECOND) as wid;

INSERT INTO test.mt VALUES (1, toDateTime('2020-01-09 12:00:01'));
INSERT INTO test.mt VALUES (2, toDateTime('2020-01-09 12:00:05'));

WATCH test.wv LIMIT 1;

INSERT INTO test.mt VALUES (3, toDateTime('2020-01-09 12:00:10'));
INSERT INTO test.mt VALUES (4, toDateTime('2020-01-09 12:00:15'));

WATCH test.wv LIMIT 1;

INSERT INTO test.mt VALUES (5, toDateTime('2020-01-09 12:00:20'));
INSERT INTO test.mt VALUES (6, toDateTime('2020-01-09 12:00:35'));

WATCH test.wv LIMIT 1;

DROP TABLE test.wv;
DROP TABLE test.mt;
