-- Tags: no-random-settings

-- It is not allowed to have experimental settings enabled by default.

-- However, some settings in the experimental tier are meant to control another experimental feature, and then they can be enabled as long as the feature itself is disabled.
-- These are in the exceptions list inside NOT IN.
-- Check `default`, not `value`: a CI configuration may enable an experimental setting in the profile.
SELECT name, default FROM system.settings WHERE tier = 'Experimental' AND type = 'Bool' AND default != '0' AND name NOT IN ('throw_on_unsupported_query_inside_transaction', 'time_series_prefer_recent_samples_table');
SELECT name, value FROM system.merge_tree_settings WHERE tier = 'Experimental' AND type = 'Bool' AND value != '0' AND name NOT IN ('remove_rolled_back_parts_immediately');
