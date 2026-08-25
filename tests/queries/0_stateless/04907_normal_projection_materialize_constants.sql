-- { echo ON }
SET optimize_use_projections = 1;

-- Normal projections are unsupported when reading over parallel replicas unless
-- `parallel_replicas_local_plan` is on and aggregation in order is off, and the runner randomizes
-- both. The assertions below require the projection to be selected, so keep the read local.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS normal_projection_materialize_constants;

-- The `PREWHERE` condition selects the all-parts normal-projection rewrite. The rewrite must
-- compare the projection output with the regular read's header and keep this compatible
-- projection selected.
CREATE TABLE normal_projection_materialize_constants
(
    k UInt64,
    v UInt64,
    PROJECTION by_v (SELECT * ORDER BY v)
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 1, max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO normal_projection_materialize_constants SELECT number, number % 2 FROM numbers(20);

-- The projection is beneficial because `by_v` narrows the `v = 1` read to 11 granules. The
-- selected projection proves the newly hoisted header comparison accepts this positive rewrite
-- instead of declining it as a structure mismatch.
SELECT count() = 1
FROM
(
    EXPLAIN projections = 1
    SELECT k FROM normal_projection_materialize_constants PREWHERE v = 1
)
WHERE explain ILIKE '%ReadFromMergeTree (by_v)%';

-- The all-parts rewrite detaches the regular read, so the plan contains no base-table read arm.
SELECT count() = 0
FROM
(
    EXPLAIN projections = 1
    SELECT k FROM normal_projection_materialize_constants PREWHERE v = 1
)
WHERE explain ILIKE '%ReadFromMergeTree%' AND explain NOT ILIKE '%(by_v)%';

SELECT groupArray(k) FROM (SELECT k FROM normal_projection_materialize_constants PREWHERE v = 1 ORDER BY k);

DROP TABLE normal_projection_materialize_constants;
