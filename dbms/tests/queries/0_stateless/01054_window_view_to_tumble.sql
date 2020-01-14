SET allow_experimental_window_view = 1;

DROP TABLE IF EXISTS test.wv;
DROP TABLE IF EXISTS test.mt;
DROP TABLE IF EXISTS test.dst;

CREATE TABLE test.mt(a Int32, timestamp DateTime) Engine=MergeTree order by tuple();
CREATE TABLE test.dst(count UInt64, w_start DateTime, w_end DateTime) Engine=MergeTree order by tuple();
CREATE WINDOW VIEW test.wv to test.dst AS SELECT count(a) as count, TUMBLE_START(wid) as w_start, TUMBLE_END(wid) as w_end FROM test.mt group by TUMBLE(timestamp, INTERVAL '5' SECOND) as wid;

INSERT INTO test.mt VALUES (1, toDateTime('2020-01-09 12:00:01'));
INSERT INTO test.mt VALUES (2, toDateTime('2020-01-09 12:00:05'));
INSERT INTO test.mt VALUES (3, toDateTime('2020-01-09 12:00:10'));
INSERT INTO test.mt VALUES (4, toDateTime('2020-01-09 12:00:15'));
INSERT INTO test.mt VALUES (5, toDateTime('2020-01-09 12:00:20'));
INSERT INTO test.mt VALUES (6, toDateTime('2020-01-09 12:00:35'));

SELECT sleep(1);

SELECT count, w_start, w_end FROM test.dst order by w_start;

DROP TABLE test.wv;
DROP TABLE test.mt;
