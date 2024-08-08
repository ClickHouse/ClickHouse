-- Tests delayed materialization of statistics in merge instead of during insert (setting 'materialize_statistics_on_insert = 0').

DROP TABLE IF EXISTS tab;

SET enable_analyzer = 1;
SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;

SET materialize_statistics_on_insert = 0;

CREATE TABLE tab
(
    a Int64 STATISTICS(tdigest),
    b Int16 STATISTICS(tdigest),
) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, enable_vertical_merge_algorithm = 0; -- TODO: there is a bug in vertical merge with statistics.

INSERT INTO tab SELECT number, -number FROM system.numbers LIMIT 10000;

SELECT count(*) FROM tab WHERE b < 10 and a < 10 SETTINGS log_comment = 'statistics not used';

OPTIMIZE TABLE tab FINAL;

SELECT count(*) FROM tab WHERE b < 10 and a < 10 SETTINGS log_comment = 'statistics used after merge';

TRUNCATE TABLE tab;
SET mutations_sync = 2;

INSERT INTO tab SELECT number, -number FROM system.numbers LIMIT 10000;
ALTER TABLE tab MATERIALIZE STATISTICS a, b;

SELECT count(*) FROM tab WHERE b < 10 and a < 10 SETTINGS log_comment = 'statistics used after materialize';

DROP TABLE tab;

SYSTEM FLUSH LOGS;

SELECT log_comment, message FROM system.text_log JOIN
(
    SELECT Settings['log_comment'] AS log_comment, query_id FROM system.query_log
    WHERE current_database = currentDatabase()
        AND query LIKE 'SELECT count(*) FROM tab%'
        AND type = 'QueryFinish'
) AS query_log USING (query_id)
WHERE message LIKE '%moved to PREWHERE%'
ORDER BY event_time_microseconds;

SELECT count(), sum(ProfileEvents['MergeTreeDataWriterStatisticsCalculationMicroseconds'])
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query LIKE 'INSERT INTO tab SELECT%'
    AND type = 'QueryFinish';
