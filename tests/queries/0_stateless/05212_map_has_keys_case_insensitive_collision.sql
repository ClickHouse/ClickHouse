-- Tags: no-fasttest

SET enable_analyzer = 1;
SET optimize_rewrite_has_to_in = 0;

-- A case-insensitive file reader can resolve m.keys to a physical M.keys column.
-- Do not rewrite has(), notHas() and mapContainsKey() when such a column shadows the Map subcolumn.
INSERT INTO FUNCTION file(currentDatabase() || '_05212_map_has_keys_ci.orc', ORC, 'm Map(String, UInt64), `M.keys` Array(String)')
SELECT map('key', toUInt64(1)), ['other']
SETTINGS engine_file_truncate_on_insert = 1;

SELECT 'optimized', countIf(has(m, 'other')), countIf(notHas(m, 'other')), countIf(mapContainsKey(m, 'other'))
FROM file(currentDatabase() || '_05212_map_has_keys_ci.orc', ORC, 'm Map(String, UInt64), `M.keys` Array(String)')
SETTINGS input_format_orc_case_insensitive_column_matching = 1, optimize_functions_to_subcolumns = 1;

SELECT 'unoptimized', countIf(has(m, 'other')), countIf(notHas(m, 'other')), countIf(mapContainsKey(m, 'other'))
FROM file(currentDatabase() || '_05212_map_has_keys_ci.orc', ORC, 'm Map(String, UInt64), `M.keys` Array(String)')
SETTINGS input_format_orc_case_insensitive_column_matching = 1, optimize_functions_to_subcolumns = 0;
