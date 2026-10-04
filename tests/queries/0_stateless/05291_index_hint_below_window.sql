-- https://github.com/ClickHouse/ClickHouse/issues/121177: an indexHint must not prune rows below a window or GROUP BY

SET optimize_and_compare_chain = 1;

DROP TABLE IF EXISTS t_hint_window;
CREATE TABLE t_hint_window (k UInt32, v UInt32, w UInt32) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 1;
INSERT INTO t_hint_window SELECT number % 10, 50 + number, 60 + number FROM numbers(100);
INSERT INTO t_hint_window SELECT number % 10, number, number + 1 FROM numbers(30);

SELECT sum(s) FROM (SELECT k, v, w, sum(v) OVER (PARTITION BY k) AS s FROM t_hint_window QUALIFY v <= w AND w < 41);
SELECT sum(s) FROM (SELECT k, v, sum(v) OVER (PARTITION BY k) AS s FROM t_hint_window QUALIFY indexHint(v < 41) AND v < 41);
-- the hinted column is not read anywhere else
SELECT sum(s) FROM (SELECT k, sum(w) OVER (PARTITION BY k) AS s FROM t_hint_window QUALIFY indexHint(v < 41));
SELECT sum(s) FROM (SELECT k, sum(w) OVER (PARTITION BY k) AS s FROM t_hint_window QUALIFY indexHint(k < 100 AND indexHint(v < 41)));

DROP TABLE t_hint_window;

-- granules hold rows of two keys, so pruning by a key hint leaves a key with part of its rows
DROP TABLE IF EXISTS t_hint_key;
CREATE TABLE t_hint_key (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_hint_key SELECT intDiv(number, 10), number FROM numbers(100);

SELECT count(), sum(s) FROM (SELECT k, count() OVER (PARTITION BY k) AS s FROM t_hint_key QUALIFY indexHint(k = 1));
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_hint_key GROUP BY k HAVING indexHint(k = 1));

DROP TABLE t_hint_key;
