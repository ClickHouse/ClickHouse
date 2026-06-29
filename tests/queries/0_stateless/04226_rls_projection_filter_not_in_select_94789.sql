-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/94789
-- Row policy + projection: query failed with `NOT_FOUND_COLUMN_IN_BLOCK`
-- when the filter column was not in the SELECT list (and `force_optimize_projection = 1`).

DROP ROW POLICY IF EXISTS bug_demo_94789_policy ON bug_demo_94789;
DROP TABLE IF EXISTS bug_demo_94789;

CREATE TABLE bug_demo_94789
(
    id UUID,
    tenant_id String,
    ts DateTime('UTC'),
    data String,
    PROJECTION pr_by_data
    (
        SELECT * ORDER BY tenant_id, data, ts, id
    )
)
ENGINE = MergeTree
ORDER BY (tenant_id, ts, id);

INSERT INTO bug_demo_94789
SELECT
    generateUUIDv4(),
    if(number % 2 = 0, 'tenant_A', 'tenant_B'),
    toDateTime('2024-01-01 00:00:00', 'UTC') + INTERVAL number SECOND,
    concat('item_', toString(number % 50))
FROM numbers(10000);

ALTER TABLE bug_demo_94789 MATERIALIZE PROJECTION pr_by_data SETTINGS mutations_sync = 2;

CREATE ROW POLICY bug_demo_94789_policy ON bug_demo_94789 USING tenant_id = 'tenant_A' TO ALL;

SELECT data, count() AS cnt
FROM bug_demo_94789
GROUP BY data
ORDER BY cnt DESC, data
LIMIT 5
SETTINGS force_optimize_projection = 1;

DROP ROW POLICY bug_demo_94789_policy ON bug_demo_94789;
DROP TABLE bug_demo_94789;
