-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN indexes output is missing on the initiator under parallel replicas.
-- EXPLAIN indexes = 1 should surface projection filtering (index-only _part_offset
-- projections), otherwise a pruned read looks like a full granule scan (#110947).
-- Do not assert absolute granule totals: part format and adaptive index_granularity_bytes
-- change them under MergeTreeSettingsRandomizer.

SET enable_analyzer = 1;
SET optimize_use_projections = 1;
SET optimize_use_projection_filtering = 1;
SET parallel_replicas_local_plan = 1;
SET optimize_aggregation_in_order = 0;
SET min_table_rows_to_use_projection_index = 0;
SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_explain_proj_filter;

CREATE TABLE t_explain_proj_filter (id UInt64, k UInt64, v String)
ENGINE = MergeTree ORDER BY id
SETTINGS allow_part_offset_column_in_projections = 1, index_granularity = 8192;

INSERT INTO t_explain_proj_filter SELECT number, cityHash64(number), toString(number) FROM numbers(100000);

ALTER TABLE t_explain_proj_filter ADD PROJECTION p_off (SELECT _part_offset ORDER BY k);
ALTER TABLE t_explain_proj_filter MATERIALIZE PROJECTION p_off SETTINGS mutations_sync = 2;

-- Projection entry is present and pruning reduces the read to a single granule.
SELECT
    countIf(line = 'Projection') AS projection_entry,
    countIf(line = 'Name: p_off') AS named,
    countIf(match(line, '^Granules: 1/\\d+$')) AS single_granule_read
FROM
(
    SELECT trimLeft(explain) AS line
    FROM (EXPLAIN indexes = 1 SELECT v FROM t_explain_proj_filter WHERE k = cityHash64(50000))
);

-- With filtering disabled, no Projection index entry.
SELECT count()
FROM
(
    SELECT trimLeft(explain) AS line
    FROM
    (
        EXPLAIN indexes = 1
        SELECT v FROM t_explain_proj_filter WHERE k = cityHash64(50000)
        SETTINGS optimize_use_projection_filtering = 0
    )
    WHERE line LIKE 'Projection' OR line LIKE 'Name: p_off'
);

DROP TABLE t_explain_proj_filter;
