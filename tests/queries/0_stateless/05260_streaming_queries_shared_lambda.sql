-- Tags: no-parallel-replicas
-- Random settings limits: optimize_move_to_prewhere=(1, 1)
-- A capture-free lambda used by both the SELECT list and WHERE of a bounded streaming read with GROUP BY ALL.

SET enable_streaming_queries = 1;

DROP TABLE IF EXISTS t_stream_shared_lambda;
CREATE TABLE t_stream_shared_lambda (date Date, data Array(UInt32)) ENGINE = MergeTree ORDER BY date;
INSERT INTO t_stream_shared_lambda VALUES ('2024-01-01', [1, 2, 3]);

SELECT date, arrayFilter(x -> (x IN (2, 3)), data) AS filtered FROM t_stream_shared_lambda STREAM BOUNDED WHERE arrayExists(x -> (x IN (2, 3)), data) GROUP BY ALL;

DROP TABLE t_stream_shared_lambda;
