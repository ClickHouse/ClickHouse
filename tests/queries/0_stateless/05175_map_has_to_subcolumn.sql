-- Test that Map key predicates use the keys subcolumn without changing results.

SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;
SET optimize_rewrite_has_to_in = 0;

DROP TABLE IF EXISTS t_map_has_subcolumn;

CREATE TABLE t_map_has_subcolumn
(
    id UInt64,
    m Map(String, String)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_map_has_subcolumn VALUES
    (0, {'service': 'api', 'debug': '1'}),
    (1, {'service': 'worker'}),
    (2, {'debug': '1'}),
    (3, {}),
    (4, {'service': 'api'});

-- All three Map key predicates must read m.keys, not m.values.
SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT id FROM t_map_has_subcolumn WHERE has(m, 'service'))
WHERE explain LIKE '%m.keys%';

SELECT count() = 0
FROM (EXPLAIN actions = 1 SELECT id FROM t_map_has_subcolumn WHERE has(m, 'service'))
WHERE explain LIKE '%m.values%';

SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT id FROM t_map_has_subcolumn WHERE notHas(m, 'debug'))
WHERE explain LIKE '%m.keys%';

SELECT count() = 0
FROM (EXPLAIN actions = 1 SELECT id FROM t_map_has_subcolumn WHERE notHas(m, 'debug'))
WHERE explain LIKE '%m.values%';

SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT id FROM t_map_has_subcolumn WHERE mapContainsKey(m, 'service'))
WHERE explain LIKE '%m.keys%';

SELECT count() = 0
FROM (EXPLAIN actions = 1 SELECT id FROM t_map_has_subcolumn WHERE mapContainsKey(m, 'service'))
WHERE explain LIKE '%m.values%';

-- The filter-only exception must preserve the full Map read for the projection.
SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT m FROM t_map_has_subcolumn WHERE has(m, 'service'))
WHERE explain LIKE '%m.keys%';

SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT m FROM t_map_has_subcolumn WHERE notHas(m, 'debug'))
WHERE explain LIKE '%m.keys%';

SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT m FROM t_map_has_subcolumn WHERE mapContainsKey(m, 'service'))
WHERE explain LIKE '%m.keys%';

SELECT id
FROM t_map_has_subcolumn
WHERE has(m, 'service')
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT id
FROM t_map_has_subcolumn
WHERE notHas(m, 'debug')
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT id
FROM t_map_has_subcolumn
WHERE has(m, 'service')
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT id
FROM t_map_has_subcolumn
WHERE notHas(m, 'debug')
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE t_map_has_subcolumn;
