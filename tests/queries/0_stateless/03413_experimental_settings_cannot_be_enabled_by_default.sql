-- Tags: no-random-settings

-- It is not allowed to have experimental settings enabled by default.

-- However, some settings in the experimental tier are meant to control another experimental feature, and then they can be enabled as long as the feature itself is disabled.
-- These are in the exceptions list inside NOT IN. `use_reader_executor` defaults to off; the
-- stateless-test config (`users.d/use_reader_executor.xml`) enables it to exercise the executor
-- read path in CI, so it shows as enabled here.
SELECT name, value FROM system.settings WHERE tier = 'Experimental' AND type = 'Bool' AND value != '0' AND name NOT IN ('throw_on_unsupported_query_inside_transaction', 'ai_function_throw_on_error', 'ai_function_throw_on_quota_exceeded', 'use_reader_executor');
SELECT name, value FROM system.merge_tree_settings WHERE tier = 'Experimental' AND type = 'Bool' AND value != '0' AND name NOT IN ('remove_rolled_back_parts_immediately');
