-- A function with a NULL constant argument returns a constant NULL, so it must not be treated as strictly monotonic in its other argument.

DROP TABLE IF EXISTS t_null_arg;
CREATE TABLE t_null_arg (dt DateTime('UTC'), y UInt8) ENGINE = MergeTree ORDER BY (dt, y);
INSERT INTO t_null_arg VALUES (0, 2), (1, 1), (2, 2), (3, 1);

SELECT toString(dt, NULL) AS s, y FROM t_null_arg ORDER BY s, y SETTINGS optimize_read_in_order = 1;
SELECT toTimeZone(dt, NULL) AS s, y FROM t_null_arg ORDER BY s, y SETTINGS optimize_read_in_order = 1;
SELECT toDateTime64(dt, NULL) AS s, y FROM t_null_arg ORDER BY s, y SETTINGS optimize_read_in_order = 1;
SELECT toString(dt, CAST(NULL, 'Nullable(String)')) AS s, y FROM t_null_arg ORDER BY s, y SETTINGS optimize_read_in_order = 1;
SELECT count() FROM (SELECT DISTINCT toString(dt, NULL), y FROM t_null_arg) SETTINGS optimize_distinct_in_order = 1;
SELECT count() FROM (SELECT toString(dt, NULL) AS s, y FROM t_null_arg GROUP BY s, y) SETTINGS optimize_aggregation_in_order = 1, optimize_injective_functions_in_group_by = 0;
SELECT y, row_number() OVER (ORDER BY toString(dt, NULL), y) AS rn FROM t_null_arg ORDER BY rn SETTINGS optimize_read_in_order = 1, query_plan_reuse_storage_ordering_for_window_functions = 1;
SELECT toString(dt, NULL) AS s, y FROM (SELECT dt, y FROM t_null_arg ORDER BY dt, y LIMIT 100) ORDER BY s, y SETTINGS optimize_sorting_by_input_stream_properties = 1;
-- A non-NULL zone keeps the strict claim, so the key still orders y.
SELECT countIf(explain ILIKE '%Prefix sort description:%y ASC%') FROM (EXPLAIN actions = 1 SELECT toTimeZone(dt, 'UTC') AS s, y FROM t_null_arg ORDER BY s, y SETTINGS optimize_read_in_order = 1);

DROP TABLE t_null_arg;
