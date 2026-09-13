-- Tags: no-fasttest

SET enable_analyzer = 1;

-- A case-insensitive file reader can resolve a flattened physical column to m.keys or m.values.
-- Do not rewrite the Map LIKE predicate when that column shadows the Map subcolumn.
INSERT INTO FUNCTION file(currentDatabase() || '_05212_map_contains_key_like_ci.orc', ORC, 'm Map(String, UInt64), `M.keys` Array(String)')
SELECT map('key', toUInt64(1)), ['not_key']
SETTINGS engine_file_truncate_on_insert = 1;

SELECT 'optimized', countIf(mapContainsKeyLike(m, 'not%'))
FROM file(currentDatabase() || '_05212_map_contains_key_like_ci.orc', ORC, 'm Map(String, UInt64), `M.keys` Array(String)')
SETTINGS input_format_orc_case_insensitive_column_matching = 1, optimize_functions_to_subcolumns = 1;

SELECT 'unoptimized', countIf(mapContainsKeyLike(m, 'not%'))
FROM file(currentDatabase() || '_05212_map_contains_key_like_ci.orc', ORC, 'm Map(String, UInt64), `M.keys` Array(String)')
SETTINGS input_format_orc_case_insensitive_column_matching = 1, optimize_functions_to_subcolumns = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_05212_map_contains_value_like_ci.orc', ORC, 'm Map(String, String), `M.values` Array(String)')
SELECT map('key', 'one'), ['four']
SETTINGS engine_file_truncate_on_insert = 1;

SELECT 'optimized', countIf(mapContainsValueLike(m, '4%'))
FROM file(currentDatabase() || '_05212_map_contains_value_like_ci.orc', ORC, 'm Map(String, String), `M.values` Array(String)')
SETTINGS input_format_orc_case_insensitive_column_matching = 1, optimize_functions_to_subcolumns = 1;

SELECT 'unoptimized', countIf(mapContainsValueLike(m, '4%'))
FROM file(currentDatabase() || '_05212_map_contains_value_like_ci.orc', ORC, 'm Map(String, String), `M.values` Array(String)')
SETTINGS input_format_orc_case_insensitive_column_matching = 1, optimize_functions_to_subcolumns = 0;
