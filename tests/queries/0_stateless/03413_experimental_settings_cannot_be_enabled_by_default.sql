-- Tags: no-random-settings

-- It is not allowed to have experimental settings enabled by default.

-- However, some settings in the experimental tier are meant to control another experimental feature, and then they can be enabled as long as the feature itself is disabled.
-- These are in the exceptions list inside NOT IN. `use_reader_executor` and
-- `reader_executor_use_long_connections` default to off; the stateless-test config
-- (`users.d/use_reader_executor.xml`) enables them to exercise the executor read
-- path in CI, so they show as enabled here.
SELECT name, value FROM system.settings WHERE tier = 'Experimental' AND type = 'Bool' AND value != '0' AND name NOT IN ('throw_on_unsupported_query_inside_transaction', 'time_series_prefer_recent_samples_table', 'use_reader_executor', 'reader_executor_use_long_connections');
SELECT name, value FROM system.merge_tree_settings WHERE tier = 'Experimental' AND type = 'Bool' AND value != '0' AND name NOT IN ('remove_rolled_back_parts_immediately');
