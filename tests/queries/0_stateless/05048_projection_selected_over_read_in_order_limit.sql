-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database, no-parallel-replicas: EXPLAIN output differs.

-- https://github.com/ClickHouse/ClickHouse/issues/116801
-- A query `WHERE grp = 'x' ORDER BY ts DESC LIMIT n` on a table with
-- `ORDER BY (ts, grp)` and a projection `ORDER BY (grp, ts)`.
-- The projection wins on the mark count, but its read must not fall back to
-- a full sort of every matching row before the `LIMIT` applies.

SET optimize_read_in_order = 1;
SET optimize_use_projections = 1;
SET optimize_use_projection_filtering = 1;
SET query_plan_optimize_lazy_materialization = 0;
SET read_in_order_use_virtual_row = 1;
SET query_plan_optimize_prewhere = 1;
SET query_plan_merge_filters = 1;
SET query_plan_remove_unused_columns = 1;
SET optimize_move_to_prewhere = 1;
SET use_top_k_dynamic_filtering = 0;

DROP TABLE IF EXISTS repro_proj_order;

CREATE TABLE repro_proj_order
(
    id Int64,
    ts DateTime64(3, 'UTC'),
    grp LowCardinality(String),
    val Float32,
    PROJECTION proj_by_grp
    (
        SELECT *
        ORDER BY grp, ts
    )
)
ENGINE = MergeTree
ORDER BY (ts, grp)
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;

INSERT INTO repro_proj_order
SELECT number AS id, toDateTime64('2026-01-01 00:00:00', 3, 'UTC') - number AS ts, if(number % 2 = 0, 'even', 'odd') AS grp, number AS val
FROM numbers_mt(1000000);

OPTIMIZE TABLE repro_proj_order FINAL;

EXPLAIN indexes = 1
SELECT id, ts FROM repro_proj_order WHERE grp = 'even' ORDER BY ts DESC LIMIT 200;

SELECT sum(id), min(ts), max(ts)
FROM
(
    SELECT id, ts FROM repro_proj_order WHERE grp = 'even' ORDER BY ts DESC LIMIT 200
);
