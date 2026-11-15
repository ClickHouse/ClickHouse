-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104568
-- Manually changing a setting via its alias name must remove it from the
-- compatibility-tracking set, so the next `SET compatibility=` doesn't revert it.

SET enable_lightweight_update = DEFAULT;

SET compatibility = '25.7';
SELECT 'after compat 25.7', name, value FROM system.settings WHERE name = 'enable_lightweight_update';

-- Set the canonical setting through its alias name.
SET allow_experimental_lightweight_update = 1;
SELECT 'after alias set', name, value FROM system.settings WHERE name = 'enable_lightweight_update';

-- Changing compatibility again must keep the manual value.
SET compatibility = '25.6';
SELECT 'after compat 25.6', name, value FROM system.settings WHERE name = 'enable_lightweight_update';
