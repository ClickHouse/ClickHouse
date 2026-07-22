-- Tags: no-parallel-replicas

-- Test that text indexes on mapKeys(m) do not cause "Not-ready Set" exception
-- when a query uses map_column['key'] IN (SELECT ... subquery).
-- The bug: traverseMapElementKeyNode clones the IN expression DAG and tries
-- to execute it, but the Set from the subquery is not built yet at that point.

SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET optimize_functions_to_subcolumns = 1;
SET optimize_use_projections = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    name LowCardinality(String),
    attrs Map(LowCardinality(String), String),
    INDEX idx_attrs_keys mapKeys(attrs) TYPE text(tokenizer = 'array'),
    INDEX idx_attrs_values mapValues(attrs) TYPE text(tokenizer = 'array')
)
ENGINE = MergeTree
ORDER BY (name, id)
SETTINGS index_granularity = 1;

INSERT INTO tab VALUES
    (0, 'name-a', {'entity':'e1', 'reason':'OK'}),
    (1, 'name-a', {'entity':'e2', 'reason':'TooManyRequests'}),
    (2, 'name-a', {'entity':'e3', 'reason':'OK'}),
    (3, 'name-b', {'entity':'e4', 'reason':'Timeout'}),
    (4, 'name-a', {'entity':'e1', 'reason':'TooManyRequests'}),
    (5, 'name-a', {'entity':'e3', 'reason':'Unknown Error'});

SELECT id
FROM tab
WHERE name = 'name-a'
    AND mapContains(attrs, 'entity')
    AND attrs['entity'] IN (
        SELECT DISTINCT attrs['entity']
        FROM tab
        WHERE name = 'name-a'
            AND mapContains(attrs, 'reason')
            AND attrs['reason'] LIKE '%TooMany%'
    )
ORDER BY id;

EXPLAIN indexes = 1
SELECT id
FROM tab
WHERE name = 'name-a'
    AND mapContains(attrs, 'entity')
    AND attrs['entity'] IN (
        SELECT DISTINCT attrs['entity']
        FROM tab
        WHERE name = 'name-a'
            AND mapContains(attrs, 'reason')
            AND attrs['reason'] LIKE '%TooMany%'
    )
ORDER BY id;

DROP TABLE tab;
