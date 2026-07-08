-- Tags: no-parallel-replicas
-- Usage counters count reading steps per server; parallel replicas would multiply them.

DROP TABLE IF EXISTS t_proj_usage;

CREATE TABLE t_proj_usage
(
    key UInt64,
    value UInt64,
    PROJECTION p_order (SELECT key, value ORDER BY value),
    PROJECTION p_agg (SELECT key, sum(value) GROUP BY key)
)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 4;

INSERT INTO t_proj_usage SELECT number, number * 10 FROM numbers(32);

SELECT 'before queries';
SELECT name, type, times_chosen, last_chosen_time
FROM system.projections
WHERE database = currentDatabase() AND table = 't_proj_usage' ORDER BY name;

SELECT key FROM t_proj_usage WHERE value = 50
SETTINGS optimize_use_projections = 1, use_query_condition_cache = 0;

SELECT 'after query using p_order';
SELECT name, type, times_chosen, last_chosen_time IS NOT NULL
FROM system.projections
WHERE database = currentDatabase() AND table = 't_proj_usage' ORDER BY name;

SELECT key, sum(value) FROM t_proj_usage GROUP BY key ORDER BY key LIMIT 1
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, use_query_condition_cache = 0;

SELECT 'after query using p_agg';
SELECT name, type, times_chosen, last_chosen_time IS NOT NULL
FROM system.projections
WHERE database = currentDatabase() AND table = 't_proj_usage' ORDER BY name;

DROP TABLE t_proj_usage;
