-- Tags: no-parallel
-- Tag no-parallel: messes with the query condition cache

-- `input_format_try_infer_ipv4` and `input_format_try_infer_ipv6` change the type that a JSON cast infers
-- without leaving a trace in the condition, so two queries that differ only in them must not share a cache entry.

-- The query condition cache is only used with the analyzer.
SET enable_analyzer = 1;
SET use_query_condition_cache = 1;
-- Without a local plan the filter steps run as part of the remote queries, and this server's cache sees nothing.
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS t_qcc_ip;

-- The auto minmax indexes would answer before the cache, and the cache stores nothing for small parts.
CREATE TABLE t_qcc_ip (k UInt64, s4 String, s6 String) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 256, add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_string_columns = 0;
INSERT INTO t_qcc_ip SELECT number, '{"ip":"192.168.1.1"}', '{"ip":"2001:db8::1"}' FROM numbers(4096);

SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_ip WHERE dynamicType(CAST(s4 AS JSON).ip) = 'IPv4' SETTINGS input_format_try_infer_ipv4 = 0;
SELECT count() FROM t_qcc_ip WHERE dynamicType(CAST(s4 AS JSON).ip) = 'IPv4' SETTINGS input_format_try_infer_ipv4 = 1;
SELECT count() FROM t_qcc_ip WHERE dynamicType(CAST(s4 AS JSON).ip) = 'IPv4' SETTINGS use_query_condition_cache = 0, input_format_try_infer_ipv4 = 1;

SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_ip WHERE dynamicType(CAST(s6 AS JSON).ip) = 'IPv6' SETTINGS input_format_try_infer_ipv6 = 0;
SELECT count() FROM t_qcc_ip WHERE dynamicType(CAST(s6 AS JSON).ip) = 'IPv6' SETTINGS input_format_try_infer_ipv6 = 1;
SELECT count() FROM t_qcc_ip WHERE dynamicType(CAST(s6 AS JSON).ip) = 'IPv6' SETTINGS use_query_condition_cache = 0, input_format_try_infer_ipv6 = 1;

DROP TABLE t_qcc_ip;
