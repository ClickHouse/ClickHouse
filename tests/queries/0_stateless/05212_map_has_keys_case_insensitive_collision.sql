-- Tags: no-fasttest

SET enable_analyzer = 1;
SET optimize_rewrite_has_to_in = 0;
SET optimize_functions_to_subcolumns = 1;

-- On a storage with case-sensitive name resolution a top-level `M.keys` column does not shadow the
-- m.keys subcolumn, so has(), notHas() and mapContainsKey() must still be rewritten and must still
-- read the Map's own keys. The same holds for mapContainsValue() next to a top-level `M.values`.
DROP TABLE IF EXISTS t_05212_map_keys_ci;
CREATE TABLE t_05212_map_keys_ci (id UInt64, m Map(String, UInt64), `M.keys` Array(String), `M.values` Array(UInt64))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05212_map_keys_ci VALUES (1, map('key', toUInt64(1)), ['other'], [toUInt64(4)]);

SELECT 'mergetree has rewritten', count() > 0
FROM (EXPLAIN actions = 1 SELECT id FROM t_05212_map_keys_ci WHERE has(m, 'other'))
WHERE explain LIKE '%has(m.keys%';

SELECT 'mergetree notHas rewritten', count() > 0
FROM (EXPLAIN actions = 1 SELECT id FROM t_05212_map_keys_ci WHERE notHas(m, 'other'))
WHERE explain LIKE '%notHas(m.keys%';

SELECT 'mergetree mapContainsKey rewritten', count() > 0
FROM (EXPLAIN actions = 1 SELECT id FROM t_05212_map_keys_ci WHERE mapContainsKey(m, 'other'))
WHERE explain LIKE '%has(m.keys%';

SELECT 'mergetree mapContainsValue rewritten', count() > 0
FROM (EXPLAIN actions = 1 SELECT id FROM t_05212_map_keys_ci WHERE mapContainsValue(m, toUInt64(4)))
WHERE explain LIKE '%has(m.values%';

SELECT 'mergetree sibling not read', count()
FROM (EXPLAIN actions = 1 SELECT id FROM t_05212_map_keys_ci WHERE has(m, 'other') AND mapContainsValue(m, toUInt64(4)))
WHERE explain LIKE '%M.keys%' OR explain LIKE '%M.values%';

SELECT 'mergetree optimized',
    countIf(has(m, 'other')), countIf(notHas(m, 'other')), countIf(mapContainsKey(m, 'other')), countIf(mapContainsValue(m, toUInt64(4))),
    countIf(has(m, 'key')), countIf(notHas(m, 'key')), countIf(mapContainsKey(m, 'key')), countIf(mapContainsValue(m, toUInt64(1)))
FROM t_05212_map_keys_ci;

SELECT 'mergetree unoptimized',
    countIf(has(m, 'other')), countIf(notHas(m, 'other')), countIf(mapContainsKey(m, 'other')), countIf(mapContainsValue(m, toUInt64(4))),
    countIf(has(m, 'key')), countIf(notHas(m, 'key')), countIf(mapContainsKey(m, 'key')), countIf(mapContainsValue(m, toUInt64(1)))
FROM t_05212_map_keys_ci
SETTINGS optimize_functions_to_subcolumns = 0;

DROP TABLE t_05212_map_keys_ci;

-- The LIKE variants use the same case-sensitive Map subcolumn resolution on MergeTree.
DROP TABLE IF EXISTS t_05212_map_like_ci;
CREATE TABLE t_05212_map_like_ci (id UInt64, m Map(String, String), `M.keys` Array(String), `M.values` Array(String))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05212_map_like_ci VALUES (1, map('key', 'one'), ['other'], ['four']);

SELECT 'mergetree mapContainsKeyLike rewritten', count() > 0
FROM (EXPLAIN actions = 1 SELECT id FROM t_05212_map_like_ci WHERE mapContainsKeyLike(m, 'other%'))
WHERE explain LIKE '%m.keys%';

SELECT 'mergetree mapContainsValueLike rewritten', count() > 0
FROM (EXPLAIN actions = 1 SELECT id FROM t_05212_map_like_ci WHERE mapContainsValueLike(m, 'four%'))
WHERE explain LIKE '%m.values%';

DROP TABLE t_05212_map_like_ci;

-- A case-insensitive file reader resolves m.keys to the physical `M.keys` column. The pass never rewrites
-- Map functions for such readers, so the results must not change with the optimization enabled.
INSERT INTO FUNCTION file(currentDatabase() || '_05212_map_has_keys_ci.orc', ORC, 'm Map(String, UInt64), `M.keys` Array(String)')
SELECT map('key', toUInt64(1)), ['other']
SETTINGS engine_file_truncate_on_insert = 1;

SELECT 'file optimized', countIf(has(m, 'other')), countIf(notHas(m, 'other')), countIf(mapContainsKey(m, 'other'))
FROM file(currentDatabase() || '_05212_map_has_keys_ci.orc', ORC, 'm Map(String, UInt64), `M.keys` Array(String)')
SETTINGS input_format_orc_case_insensitive_column_matching = 1, optimize_functions_to_subcolumns = 1;

SELECT 'file unoptimized', countIf(has(m, 'other')), countIf(notHas(m, 'other')), countIf(mapContainsKey(m, 'other'))
FROM file(currentDatabase() || '_05212_map_has_keys_ci.orc', ORC, 'm Map(String, UInt64), `M.keys` Array(String)')
SETTINGS input_format_orc_case_insensitive_column_matching = 1, optimize_functions_to_subcolumns = 0;
