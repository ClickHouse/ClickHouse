-- Checks the structure of system.metric_log: profile events and current metrics
-- are stored in 128 bucket columns of type Map(Enum16(...), Int64),
-- and every metric is also accessible through an ALIAS column.

SYSTEM FLUSH LOGS metric_log;

SELECT
    countIf(type LIKE 'Map(Enum16%' AND default_kind = ''),
    countIf(default_kind = 'ALIAS' AND name LIKE 'ProfileEvent\_%') > 1000,
    countIf(default_kind = 'ALIAS' AND name LIKE 'CurrentMetric\_%') > 100
FROM system.columns
WHERE database = 'system' AND table = 'metric_log';

-- The aliases read from the maps; a missing key reads as zero.
SELECT sum(ProfileEvent_Query) > 0, max(CurrentMetric_GlobalThread) > 0 FROM system.metric_log;

-- Reading every alias column works; this validates that each alias refers to a name
-- present in the Enum of its bucket (an inconsistency would throw UNKNOWN_ELEMENT_OF_ENUM).
SELECT * FROM system.metric_log ORDER BY event_time DESC LIMIT 1 FORMAT Null SETTINGS asterisk_include_alias_columns = 1;
