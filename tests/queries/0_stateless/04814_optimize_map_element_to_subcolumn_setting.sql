-- Test: the setting `optimize_map_element_to_subcolumn` controls rewriting m['key'] to the Map key subcolumn m.key_<key>.
-- The rewrite is disabled by default; only when the setting is enabled does m['key'] become m.key_<key>.
SET explain_query_plan_default = 'legacy';

SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_map_element_setting;

CREATE TABLE t_map_element_setting (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_map_element_setting SELECT number, map('key1', number, 'key2', number + 1) FROM numbers(10);

SELECT '-- Disabled by default: m[key1] is NOT rewritten to a subcolumn';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_element_setting WHERE m['key1'] > 5) WHERE explain LIKE '%m.key_key1%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_element_setting WHERE m['key1'] > 5) WHERE explain LIKE '%arrayElement%';

SELECT '-- Enabled: m[key1] is rewritten to the m.key_key1 subcolumn';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_element_setting WHERE m['key1'] > 5 SETTINGS optimize_map_element_to_subcolumn = 1) WHERE explain LIKE '%m.key_key1%';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_element_setting WHERE m['key1'] > 5 SETTINGS optimize_map_element_to_subcolumn = 1) WHERE explain LIKE '%arrayElement%';

SELECT '-- The setting has no effect when optimize_functions_to_subcolumns is disabled';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT id FROM t_map_element_setting WHERE m['key1'] > 5 SETTINGS optimize_functions_to_subcolumns = 0, optimize_map_element_to_subcolumn = 1) WHERE explain LIKE '%m.key_key1%';

SELECT '-- Other Map optimizations (mapKeys/mapValues/length) are unaffected by the setting';
-- length(m) -> m.size0 still applies with the map-element rewrite disabled.
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT length(m) FROM t_map_element_setting) WHERE explain LIKE '%m.size0%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT mapKeys(m) FROM t_map_element_setting) WHERE explain LIKE '%m.keys%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT mapValues(m) FROM t_map_element_setting) WHERE explain LIKE '%m.values%';

SELECT '-- Results are identical whether the setting is enabled or disabled';
SELECT id, m['key1'], m['key2'] FROM t_map_element_setting WHERE m['key1'] >= 7 ORDER BY id
SETTINGS optimize_map_element_to_subcolumn = 0;
SELECT id, m['key1'], m['key2'] FROM t_map_element_setting WHERE m['key1'] >= 7 ORDER BY id
SETTINGS optimize_map_element_to_subcolumn = 1;

DROP TABLE t_map_element_setting;
