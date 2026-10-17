-- Real-time previews of the query result (the `query_result_previews` setting) must never change
-- the result of a query: preview blocks travel out of band (`PreviewData` packets), are ignored by
-- non-interactive clients, and are excluded from limits, quotas, and counters. The frequency
-- thresholds are zeroed here so previews fire on every consumed block, exercising the snapshot,
-- merge, and pass-through machinery of every shape below.

SET query_result_previews = 1;
SET query_result_previews_min_interval_ms = 0;
SET max_threads = 4;
SET max_block_size = 65536;

SELECT '-- aggregation without keys';
SELECT count(), sum(number), max(number) FROM numbers(1000000);

SELECT '-- aggregation with a numeric key';
SELECT intDiv(number, 100000) AS k, count() AS c FROM numbers(1000000) GROUP BY k ORDER BY k;

SELECT '-- aggregation with a string key and a stateful aggregate function';
SELECT toString(number % 3) AS k, uniqExact(number % 1000) AS u FROM numbers(1000000) GROUP BY k ORDER BY k;

SELECT '-- aggregation with a nullable key';
SELECT nullIf(number % 3, 2) AS k, count() AS c FROM numbers(1000000) GROUP BY k ORDER BY k NULLS LAST;

SELECT '-- HAVING, LIMIT, and OFFSET apply to previews and to the result alike';
SELECT intDiv(number, 100000) AS k, count() AS c FROM numbers(1000000) GROUP BY k HAVING k >= 3 ORDER BY k LIMIT 4 OFFSET 2;

SELECT '-- LIMIT WITH TIES';
SELECT count() FROM (SELECT number % 5 AS k, count() AS c FROM numbers(1000000) GROUP BY k ORDER BY c LIMIT 2 WITH TIES);

SELECT '-- sorting with a limit';
SELECT number AS n FROM numbers(1000000) ORDER BY intHash64(n) % 1000 = 0 DESC, n LIMIT 3;

SELECT '-- aggregation followed by sorting with a limit';
SELECT number % 1000 AS k, count() AS c FROM numbers(1000000) GROUP BY k ORDER BY k DESC LIMIT 3;

SELECT '-- states above the thresholds stop previews but not the query';
SELECT number AS k FROM numbers(1000000) GROUP BY k ORDER BY k LIMIT 3 SETTINGS query_result_previews_max_result_rows = 10;

SELECT '-- DISTINCT applies to previews standalone';
SELECT DISTINCT number % 3 AS k FROM numbers(1000000) ORDER BY k;
SELECT DISTINCT intDiv(number, 100000) AS k FROM numbers(1000000) ORDER BY k LIMIT 3;
SELECT DISTINCT c FROM (SELECT intDiv(number, 100000) AS k, count() AS c FROM numbers(1000000) GROUP BY k) ORDER BY c;

SELECT '-- window functions apply to previews standalone';
SELECT k, c, bar(c, 0, max(c) OVER (), 10) FROM (SELECT intDiv(number, 250000) AS k, count() AS c FROM numbers(1000000) GROUP BY k) ORDER BY k;
SELECT k, round(c / max(c) OVER (), 2) AS share FROM (SELECT intDiv(number, 500000) AS k, count() AS c FROM numbers(1000000) GROUP BY k) ORDER BY k;

SELECT '-- shapes for which previews are not emitted';
SELECT number % 2 AS k, count() AS c FROM numbers(100000) GROUP BY k WITH TOTALS ORDER BY k;
SELECT number % 2 AS k, count() AS c FROM numbers(100000) GROUP BY ROLLUP(k) ORDER BY k, c;

SELECT '-- an aggregation feeding another consumer stays dormant';
SELECT count() FROM numbers(10) WHERE number IN (SELECT number % 5 FROM numbers(100000) GROUP BY number % 5);

SELECT '-- UNION of two aggregations';
SELECT sum(c) FROM (SELECT count() AS c FROM numbers(500000) UNION ALL SELECT count() AS c FROM numbers(500000));

SELECT '-- distributed query (previews are disabled for the remote parts)';
SELECT intDiv(number, 100000) AS k, count() AS c FROM remote('127.0.0.{1,2}', numbers(500000)) GROUP BY k ORDER BY k;

SELECT '-- INSERT SELECT keeps preview emitters dormant';
DROP TABLE IF EXISTS t_05038_previews;
CREATE TABLE t_05038_previews (k UInt64, c UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05038_previews SELECT intDiv(number, 100000) AS k, count() AS c FROM numbers(1000000) GROUP BY k;
SELECT count(), sum(c) FROM t_05038_previews;
DROP TABLE t_05038_previews;

SELECT '-- max_result_rows is not tripped by preview rows';
SELECT intDiv(number, 100000) AS k, count() AS c FROM numbers(1000000) GROUP BY k ORDER BY k
    SETTINGS max_result_rows = 10, result_overflow_mode = 'throw';
