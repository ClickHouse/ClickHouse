-- Tags: no-fasttest

DROP TABLE IF EXISTS part_profile_events SYNC;

CREATE TABLE part_profile_events (key UInt32, value String) Engine=MergeTree ORDER BY key SETTINGS disk = 's3_disk';
INSERT INTO part_profile_events SELECT number, toString(number) FROM numbers(100);

SYSTEM FLUSH LOGS part_log;

SELECT count() FROM part_profile_events;
SELECT ProfileEvents['S3PutObject'] > 0 FROM system.part_log where event_time >= now() - interval 2 minute and table = 'part_profile_events' and database = currentDatabase();
