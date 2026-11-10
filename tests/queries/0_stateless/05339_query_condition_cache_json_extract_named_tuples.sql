-- Queries that differ only in `json_extract_named_tuples_as_objects` must not share a query condition cache entry.

DROP TABLE IF EXISTS t_qcc_json_named_tuples;
SET enable_analyzer = 1;
SET use_query_condition_cache = 1;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS t_qcc_json_named_tuples;
CREATE TABLE t_qcc_json_named_tuples (j String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_qcc_json_named_tuples SELECT '{"t":["a","b"]}' FROM numbers(100000);

-- The named tuple stays at its defaults with the setting (no row matches)
-- and is filled from the array without it (every row matches).
SELECT count() FROM t_qcc_json_named_tuples WHERE JSONExtract(j, 't', 'Tuple(x String, y String)').x = 'a' SETTINGS json_extract_named_tuples_as_objects = 1;
SELECT count() FROM t_qcc_json_named_tuples WHERE JSONExtract(j, 't', 'Tuple(x String, y String)').x = 'a' SETTINGS json_extract_named_tuples_as_objects = 0;
SELECT count() FROM t_qcc_json_named_tuples WHERE JSONExtract(j, 't', 'Tuple(x String, y String)').x = 'a' SETTINGS use_query_condition_cache = 0, json_extract_named_tuples_as_objects = 0;

DROP TABLE t_qcc_json_named_tuples;
