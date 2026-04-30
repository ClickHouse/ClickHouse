-- Tags: no-random-settings

-- It is not allowed to have experimental settings enabled by default.

-- However, some settings in the experimental tier are meant to control another experimental feature, and then they can be enabled as long as the feature itself is disabled.
-- These are in the exceptions list inside NOT IN.
<<<<<<< HEAD
SELECT name, value FROM system.settings WHERE tier = 'Experimental' AND type = 'Bool' AND value != '0' AND name NOT IN ('throw_on_unsupported_query_inside_transaction', 'ai_function_throw_on_error', 'ai_function_throw_on_quota_exceeded');
=======
SELECT name, value FROM system.settings WHERE tier = 'Experimental' AND type = 'Bool' AND value != '0' AND name NOT IN (
  'throw_on_unsupported_query_inside_transaction',
-- turned ON for Altinity Antalya builds specifically
  'allow_experimental_iceberg_read_optimization'
);
>>>>>>> 8c8b170f4cc (Merge pull request #1687 from Altinity/feature/antalya-26.3/pr-1414-1)
SELECT name, value FROM system.merge_tree_settings WHERE tier = 'Experimental' AND type = 'Bool' AND value != '0' AND name NOT IN ('remove_rolled_back_parts_immediately');
