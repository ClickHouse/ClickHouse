-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104568
-- Manually changing a setting via its alias name must remove it from the
-- compatibility-tracking set, so the next `SET compatibility=` doesn't revert it.

SET compatibility = '24.1';
SELECT 'after compat 24.1', name, value FROM system.settings WHERE name = 'allow_experimental_analyzer';

-- Set the canonical setting through its alias name.
SET enable_analyzer = 1;
SELECT 'after alias set', name, value FROM system.settings WHERE name = 'allow_experimental_analyzer';

-- Changing compatibility again must keep the manual value.
SET compatibility = '24.2';
SELECT 'after compat 24.2', name, value FROM system.settings WHERE name = 'allow_experimental_analyzer';
