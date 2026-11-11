-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.
-- Random settings limits: query_plan_direct_read_from_text_index=(1, None); query_plan_text_index_add_hint=(1, None)
-- A `mapKeys` or `JSONAllPaths` text index adds a key hint to a predicate over a map element or JSON path.
-- The hint may only decide which rows pass: the value of the expression must not change.

SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS tab_map;
CREATE TABLE tab_map (id UInt8, m Map(String, UInt8), INDEX idx mapKeys(m) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_map VALUES (1, {'k': 5}), (2, {'k': 2}), (3, {'x': 3}), (5, {'k': 5}), (7, {'k': 0});

SELECT id, m['k'] FROM tab_map WHERE m['k'] > 0 ORDER BY id;
SELECT sum(m['k']) FROM tab_map WHERE m['k'] > 0;
SELECT id FROM tab_map WHERE m['k'] > 0 AND bitAnd(m['k'], 7) = id ORDER BY id;
SELECT id, m['k'] AS v FROM tab_map WHERE v ORDER BY id;
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id, m['k'] FROM tab_map WHERE m['k'] > 0) WHERE explain ILIKE '%__text_index_idx_mapContainsKey%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab_map WHERE m['k'] > 0 AND bitAnd(m['k'], 7) = id SETTINGS optimize_move_to_prewhere = 0) WHERE explain ILIKE '%__text_index_idx_mapContainsKey%';

DROP TABLE IF EXISTS tab_json;
CREATE TABLE tab_json (id UInt8, j JSON, INDEX idx JSONAllPaths(j) TYPE text(tokenizer = array))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO tab_json VALUES (1, '{"k": 5}'), (2, '{"k": 2}'), (3, '{"x": 3}'), (5, '{"k": 5}');

SELECT id FROM tab_json WHERE j.k.:Int64 > 0 AND toUInt8(j.k.:Int64) = id ORDER BY id;
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM tab_json WHERE j.k.:Int64 > 0 AND toUInt8(j.k.:Int64) = id SETTINGS optimize_move_to_prewhere = 0) WHERE explain ILIKE '%__text_index_idx_JSONPathExists%';

DROP TABLE tab_map;
DROP TABLE tab_json;
