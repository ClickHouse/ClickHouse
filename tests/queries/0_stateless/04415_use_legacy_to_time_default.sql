-- Verify the default behavior of `toTime` after `use_legacy_to_time` was flipped to 0 by default.

SET session_timezone = 'UTC';

-- By default (use_legacy_to_time = 0), toTime converts the value into the Time data type.
SELECT 'Default';
WITH toTime(toDateTime(12)) AS a
SELECT toTypeName(a), a;

-- An old compatibility version restores the legacy toTime (fixed date, DateTime result),
-- because the default change is registered in SettingsChangesHistory under version 26.7.
SET compatibility = '26.6';
SELECT 'compatibility = 26.6';
WITH toTime(toDateTime(12)) AS a
SELECT toTypeName(a), a;
