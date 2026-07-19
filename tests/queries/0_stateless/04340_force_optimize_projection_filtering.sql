-- Index-only (_part_offset) projections used via projection filtering must satisfy
-- force_optimize_projection / force_optimize_projection_name (see #110946).

SET enable_analyzer = 1;
SET optimize_use_projections = 1;
SET optimize_use_projection_filtering = 1;
SET parallel_replicas_local_plan = 1;
SET optimize_aggregation_in_order = 0;
SET min_table_rows_to_use_projection_index = 0;
SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_force_proj_filter;

CREATE TABLE t_force_proj_filter (id UInt64, k UInt64, v String)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_part_offset_column_in_projections = 1, index_granularity = 8192;

INSERT INTO t_force_proj_filter SELECT number, cityHash64(number), toString(number) FROM numbers(100000);

ALTER TABLE t_force_proj_filter ADD PROJECTION p_off (SELECT _part_offset ORDER BY k);
ALTER TABLE t_force_proj_filter MATERIALIZE PROJECTION p_off SETTINGS mutations_sync = 2;

-- Projection filtering is applied (EXPLAIN) and force settings accept it as "used".
SELECT trimLeft(explain)
FROM (EXPLAIN projections = 1 SELECT v FROM t_force_proj_filter WHERE k = cityHash64(50000))
WHERE trimLeft(explain) LIKE 'Name:%' OR trimLeft(explain) LIKE 'Description:%';

SELECT v FROM t_force_proj_filter WHERE k = cityHash64(50000) SETTINGS force_optimize_projection_name = 'p_off';

SELECT v FROM t_force_proj_filter WHERE k = cityHash64(50000) SETTINGS force_optimize_projection = 1;

-- Still errors when the named projection cannot be used / does not exist.
SELECT v FROM t_force_proj_filter WHERE k = cityHash64(50000) SETTINGS force_optimize_projection_name = 'missing'; -- { serverError INCORRECT_DATA }

-- With filtering disabled, index-only projection is not used.
SELECT v FROM t_force_proj_filter WHERE k = cityHash64(50000)
SETTINGS force_optimize_projection_name = 'p_off', optimize_use_projection_filtering = 0; -- { serverError INCORRECT_DATA }

SELECT v FROM t_force_proj_filter WHERE k = cityHash64(50000)
SETTINGS force_optimize_projection = 1, optimize_use_projection_filtering = 0; -- { serverError PROJECTION_NOT_USED }

DROP TABLE t_force_proj_filter;
