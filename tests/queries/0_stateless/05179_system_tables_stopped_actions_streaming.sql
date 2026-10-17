-- Tags: no-fasttest
-- `Kafka` is not built in fast tests. No broker or dependent view is needed to test local controls.
SET send_logs_level = 'fatal';

CREATE TABLE stopped_actions_stream (x UInt64) ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:10000', kafka_topic_list = 'stopped_actions',
         kafka_group_name = 'stopped_actions', kafka_format = 'JSONEachRow';

SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_stream';
SYSTEM PAUSE stopped_actions_stream;
SYSTEM PAUSE stopped_actions_stream;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_stream';
SYSTEM STOP stopped_actions_stream;
SYSTEM STOP stopped_actions_stream;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_stream';
SYSTEM START stopped_actions_stream;
SYSTEM START stopped_actions_stream;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_stream';
SYSTEM CANCEL stopped_actions_stream;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_stream';

SYSTEM STOP stopped_actions_stream;
RENAME TABLE stopped_actions_stream TO stopped_actions_stream_renamed;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_stream_renamed';
SYSTEM START stopped_actions_stream_renamed;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_stream_renamed';
SYSTEM PAUSE stopped_actions_stream_renamed;
DETACH TABLE stopped_actions_stream_renamed SYNC;
ATTACH TABLE stopped_actions_stream_renamed;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_stream_renamed';

DROP TABLE stopped_actions_stream_renamed SYNC;
