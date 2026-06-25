SET allow_statistics = 1;

DROP TABLE IF EXISTS statistics_on_insert_profile_events;

CREATE TABLE statistics_on_insert_profile_events
(
    a UInt64 STATISTICS(minmax, uniq),
    b Nullable(UInt32) STATISTICS(basic),
    c String
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS auto_statistics_types = '';

SET log_comment = '04341_stats_on_insert_enabled';
INSERT INTO statistics_on_insert_profile_events
SELECT number, if(number % 5 = 0, NULL, number % 7), toString(number)
FROM numbers(1000)
SETTINGS materialize_statistics_on_insert = 1;

SET log_comment = '04341_stats_on_insert_disabled';
INSERT INTO statistics_on_insert_profile_events
SELECT number, number % 7, toString(number)
FROM numbers(1000, 1000)
SETTINGS materialize_statistics_on_insert = 0;

SET log_comment = '';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['MergeTreeDataWriterInsertStatisticsBlocks'] = 1,
    ProfileEvents['MergeTreeDataWriterInsertStatisticsRows'] = 1000,
    ProfileEvents['MergeTreeDataWriterInsertStatisticsColumns'] = 2,
    ProfileEvents['MergeTreeDataWriterInsertStatisticsObjects'] = 3,
    ProfileEvents['MergeTreeDataWriterInsertStatisticsBytes'] > 0,
    ProfileEvents['MergeTreeDataWriterInsertStatisticsCalculationMicroseconds'] > 0
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = '04341_stats_on_insert_enabled'
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT
    ProfileEvents['MergeTreeDataWriterInsertStatisticsBlocks'],
    ProfileEvents['MergeTreeDataWriterInsertStatisticsRows'],
    ProfileEvents['MergeTreeDataWriterInsertStatisticsColumns'],
    ProfileEvents['MergeTreeDataWriterInsertStatisticsObjects'],
    ProfileEvents['MergeTreeDataWriterInsertStatisticsBytes'],
    ProfileEvents['MergeTreeDataWriterInsertStatisticsCalculationMicroseconds']
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = '04341_stats_on_insert_disabled'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE statistics_on_insert_profile_events;
