-- Tags: no-random-settings

-- It is not allowed to have experimental settings enabled by default.

-- However, some settings in the experimental tier are meant to control another experimental feature, and then they can be enabled as long as the feature itself is disabled.
-- These are in the exceptions list inside NOT IN. `reader_executor_use_live_connections` only takes effect when the experimental `use_reader_executor` (off by default) is enabled.
SELECT name, value FROM system.settings WHERE tier = 'Experimental' AND type = 'Bool' AND value != '0' AND name NOT IN ('throw_on_unsupported_query_inside_transaction', 'ai_function_throw_on_error', 'ai_function_throw_on_quota_exceeded', 'use_reader_executor', 'reader_executor_use_live_connections');
SELECT name, value FROM system.merge_tree_settings WHERE tier = 'Experimental' AND type = 'Bool' AND value != '0' AND name NOT IN ('remove_rolled_back_parts_immediately');
