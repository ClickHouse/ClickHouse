-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database, no-parallel-replicas: EXPLAIN output differs.

-- https://github.com/ClickHouse/ClickHouse/issues/116801 (second repro from the comments)
-- The table `ORDER BY (source_id, date, have_reply, id)` satisfies the query's
-- `ORDER BY` on all 4 columns, while the projection key `(source_id, date, id)`
-- omits `have_reply`, so its usable sort prefix stops at 2 columns and forces
-- a real `Sorting` pass over every matching row before the `LIMIT` applies.
-- The projection must not be picked over the base table on the mark count alone.

SET optimize_read_in_order = 1;
SET optimize_use_projections = 1;
SET optimize_use_projection_filtering = 1;
SET query_plan_optimize_lazy_materialization = 0;
SET read_in_order_use_virtual_row = 1;
SET query_plan_optimize_prewhere = 1;
SET query_plan_merge_filters = 1;
SET query_plan_remove_unused_columns = 1;
SET use_top_k_dynamic_filtering = 0;
SET enable_multiple_prewhere_read_steps = 1;
SET allow_reorder_prewhere_conditions = 1;

DROP TABLE IF EXISTS repro_t;

CREATE TABLE repro_t
(
    id String,
    source_id String,
    date Date,
    have_reply Bool,
    content String,
    metadata String,
    extra1 String,
    extra2 String,
    extra3 String,
    PROJECTION proj_narrow
    (
        SELECT id, source_id, date, have_reply, content, metadata
        ORDER BY source_id, date, id
    )
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (source_id, date, have_reply, id)
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;

INSERT INTO repro_t
SELECT
    toString(number) AS id,
    'A' AS source_id,
    toDate('2026-01-01') + (number % 20) AS date,
    (number % 2) = 0 AS have_reply,
    repeat('x', 1000) AS content,
    repeat('y', 200) AS metadata,
    toString(number) AS extra1,
    toString(number) AS extra2,
    toString(number) AS extra3
FROM numbers(200000);

OPTIMIZE TABLE repro_t FINAL;

EXPLAIN indexes = 1
SELECT count() AS rows, sum(length(content)) AS content_bytes, sum(length(metadata)) AS metadata_bytes
FROM
(
    SELECT source_id, content, metadata, date, have_reply, id
    FROM repro_t
    PREWHERE (source_id = 'A')
        AND ((source_id, date, have_reply, id) <= ('A', toDate('2026-01-15'), false, '150000'))
    ORDER BY source_id ASC, date ASC, have_reply ASC, id ASC
    LIMIT 5001
);

SELECT count() AS rows, sum(length(content)) AS content_bytes, sum(length(metadata)) AS metadata_bytes
FROM
(
    SELECT source_id, content, metadata, date, have_reply, id
    FROM repro_t
    PREWHERE (source_id = 'A')
        AND ((source_id, date, have_reply, id) <= ('A', toDate('2026-01-15'), false, '150000'))
    ORDER BY source_id ASC, date ASC, have_reply ASC, id ASC
    LIMIT 5001
);
