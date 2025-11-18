SET allow_experimental_time_time64_type = 1;
SET use_legacy_to_time = 0;

SET session_timezone = 'Antarctica/DumontDUrville';
SELECT toTime64(toDateTime(1200000), 3);
SELECT toDateTime(1200000);

SET session_timezone = 'Cuba';
SELECT toTime64(reinterpret(toUInt64(12345), 'DateTime64(0)'), 3);
SELECT toTime(reinterpret(toUInt64(12345), 'DateTime64(0)'));
SELECT toDateTime64(12345, 0);
