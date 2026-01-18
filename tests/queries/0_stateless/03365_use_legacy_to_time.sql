SET session_timezone = 'UTC';
SET use_legacy_to_time = 0;
SELECT 'Time Type Result';
WITH toTime(toDateTime(12)) AS a
SELECT toTypeName(a), a;

SET use_legacy_to_time = 1;
SELECT 'DateTime Type Result';
WITH toTime(toDateTime(12)) AS a
SELECT toTypeName(a), a;
