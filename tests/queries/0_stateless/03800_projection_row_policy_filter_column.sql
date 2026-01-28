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

CREATE ROW POLICY rls_policy ON test_rls_projection
FOR SELECT
USING tenant_id = 'tenant_A'
TO default;

-- Verify projection is used
EXPLAIN indexes = 1
SELECT data, count() as cnt
FROM test_rls_projection
GROUP BY data
ORDER BY data
SETTINGS force_optimize_projection = 1;

-- Query without tenant_id in SELECT - should work with RLS filter applied via projection
SELECT data, count() as cnt
FROM test_rls_projection
GROUP BY data
ORDER BY data
SETTINGS force_optimize_projection = 1;

DROP ROW POLICY rls_policy ON test_rls_projection;
DROP TABLE test_rls_projection;
