-- Tests for the `strict_sql_compatibility` supersetting.
-- When enabled, it flips a curated list of existing settings to their SQL-standard values.
-- Disabling it reverts only the settings the supersetting itself changed, leaving manual user overrides alone.

-- { echo }

SELECT name, value FROM system.settings WHERE name IN ('data_type_default_nullable', 'group_by_use_nulls', 'intersect_default_mode', 'except_default_mode', 'joined_subquery_requires_alias', 'join_use_nulls', 'union_default_mode', 'aggregate_functions_null_for_empty', 'cast_keep_nullable', 'prefer_column_name_to_alias') ORDER BY name;
SET strict_sql_compatibility = 1;
SELECT name, value FROM system.settings WHERE name IN ('data_type_default_nullable', 'group_by_use_nulls', 'intersect_default_mode', 'except_default_mode', 'joined_subquery_requires_alias', 'join_use_nulls', 'union_default_mode', 'aggregate_functions_null_for_empty', 'cast_keep_nullable', 'prefer_column_name_to_alias') ORDER BY name;
SET join_use_nulls = 0;
SELECT value FROM system.settings WHERE name = 'join_use_nulls';
SET strict_sql_compatibility = 0;
SELECT name, value FROM system.settings WHERE name IN ('data_type_default_nullable', 'group_by_use_nulls', 'intersect_default_mode', 'except_default_mode', 'joined_subquery_requires_alias', 'join_use_nulls', 'union_default_mode', 'aggregate_functions_null_for_empty', 'cast_keep_nullable', 'prefer_column_name_to_alias') ORDER BY name;
SET cast_keep_nullable = 0;
SET strict_sql_compatibility = 1;
SELECT value FROM system.settings WHERE name = 'cast_keep_nullable';
SELECT value FROM system.settings WHERE name = 'union_default_mode';
