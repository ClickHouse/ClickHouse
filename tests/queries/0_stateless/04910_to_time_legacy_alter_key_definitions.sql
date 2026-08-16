-- ALTER key definitions must use the same persisted legacy spelling as CREATE definitions.

SET allow_experimental_time_time64_type = 1;
SET use_legacy_to_time = 1;

DROP TABLE IF EXISTS t_totime_alter_key;

CREATE TABLE t_totime_alter_key (c0 DateTime, c1 UInt32, v UInt32)
ENGINE = MergeTree() ORDER BY (toTime(c0), c1);

ALTER TABLE t_totime_alter_key MODIFY ORDER BY (toTime(c0), c1);
SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_totime_alter_key';

ALTER TABLE t_totime_alter_key MODIFY TTL c0 + INTERVAL 1 DAY GROUP BY toTime(c0), c1 SET v = max(v);
SELECT extract(create_table_query, 'GROUP BY toTime[A-Za-z]*')
FROM system.tables WHERE database = currentDatabase() AND name = 't_totime_alter_key';

DROP TABLE t_totime_alter_key;
