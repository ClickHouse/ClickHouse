SELECT 'select with discard enabled';
SELECT number FROM numbers(3) SETTINGS discard_query_data = 1;

SELECT 'select with discard disabled';
SELECT number FROM numbers(3) SETTINGS discard_query_data = 0;

SELECT 'empty result, totals and extremes with discard enabled';
SELECT * FROM (SELECT 1) WHERE 0 SETTINGS discard_query_data = 1;
SELECT number % 2 AS k, count() AS c FROM numbers(10) GROUP BY k WITH TOTALS ORDER BY k SETTINGS discard_query_data = 1, extremes = 1;

SELECT 'exception still propagates';
SELECT throwIf(1) SETTINGS discard_query_data = 1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SELECT 'the header is still sent';
SELECT number FROM numbers(3) FORMAT TSVWithNames SETTINGS discard_query_data = 1;

DROP TABLE IF EXISTS t_04336_discard_query_data;
CREATE TABLE t_04336_discard_query_data (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO t_04336_discard_query_data SETTINGS discard_query_data = 1 VALUES (42);

SELECT 'insert with discard enabled, and the setting is not propagated to remote subqueries feeding the initiator';
INSERT INTO t_04336_discard_query_data SETTINGS discard_query_data = 1 SELECT n + 1 FROM remote('127.0.0.2', currentDatabase(), t_04336_discard_query_data);
SELECT n FROM t_04336_discard_query_data ORDER BY n;

SELECT 'distributed query is executed fully, only the final result is discarded';
SELECT count() FROM remote('127.0.0.2', currentDatabase(), t_04336_discard_query_data) SETTINGS discard_query_data = 1;

DROP TABLE t_04336_discard_query_data;
