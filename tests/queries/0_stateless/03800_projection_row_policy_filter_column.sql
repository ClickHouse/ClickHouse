DROP TABLE IF EXISTS test_rls_projection;
DROP ROW POLICY IF EXISTS rls_policy ON test_rls_projection;

CREATE TABLE test_rls_projection
(
    id UInt64,
    tenant_id String,
    data String,

    PROJECTION proj_by_data
    (
        SELECT * ORDER BY tenant_id, data, id
    )
)
ENGINE = MergeTree()
ORDER BY (tenant_id, id);

INSERT INTO test_rls_projection VALUES
    (1, 'tenant_A', 'item_1'),
    (2, 'tenant_A', 'item_2'),
    (3, 'tenant_A', 'item_1'),
    (4, 'tenant_B', 'item_1'),
    (5, 'tenant_B', 'item_3');

ALTER TABLE test_rls_projection MATERIALIZE PROJECTION proj_by_data;

-- Baseline without any row policy: all rows are visible, so 'item_1' is counted 3 times
-- (twice for tenant_A and once for tenant_B) and 'item_3' is present.
SELECT data, count() as cnt
FROM test_rls_projection
GROUP BY data
ORDER BY data
SETTINGS force_optimize_projection = 1, optimize_use_projections = 1;

CREATE ROW POLICY rls_policy ON test_rls_projection
FOR SELECT
USING tenant_id = 'tenant_A'
TO default;

-- Verify the projection is used and the row policy filter is pushed down into the projection read.
EXPLAIN indexes = 1
SELECT data, count() as cnt
FROM test_rls_projection
GROUP BY data
ORDER BY data
SETTINGS force_optimize_projection = 1, optimize_use_projections = 1, enable_analyzer = 1;

-- Query without tenant_id in SELECT - should work with the RLS filter applied via the projection.
-- The result must differ from the baseline: 'item_1' is now counted only twice (tenant_A rows)
-- and 'item_3' (a tenant_B row) is filtered out, proving the row policy filter is applied.
SELECT data, count() as cnt
FROM test_rls_projection
GROUP BY data
ORDER BY data
SETTINGS force_optimize_projection = 1, optimize_use_projections = 1;

DROP ROW POLICY rls_policy ON test_rls_projection;
DROP TABLE test_rls_projection;
