-- Tags: no-fasttest

SET enable_analyzer = 1;

-- A case-insensitive file reader can resolve m.values to a physical M.values column.
-- Do not rewrite mapContainsValue() when such a column shadows the Map subcolumn.
INSERT INTO FUNCTION file(currentDatabase() || '_05210_map_contains_value_ci.orc', ORC, 'm Map(String, UInt64), `M.values` Array(UInt64)')
SELECT map('key', toUInt64(1)), [toUInt64(4)]
SETTINGS engine_file_truncate_on_insert = 1;

SELECT 'optimized', countIf(mapContainsValue(m, toUInt64(4)))
FROM file(currentDatabase() || '_05210_map_contains_value_ci.orc', ORC, 'm Map(String, UInt64), `M.values` Array(UInt64)')
SETTINGS input_format_orc_case_insensitive_column_matching = 1, optimize_functions_to_subcolumns = 1;

SELECT 'unoptimized', countIf(mapContainsValue(m, toUInt64(4)))
FROM file(currentDatabase() || '_05210_map_contains_value_ci.orc', ORC, 'm Map(String, UInt64), `M.values` Array(UInt64)')
SETTINGS input_format_orc_case_insensitive_column_matching = 1, optimize_functions_to_subcolumns = 0;
