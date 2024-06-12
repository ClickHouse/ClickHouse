DROP TABLE IF EXISTS t_statistics_materialize;

SET allow_experimental_analyzer = 1;
SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;
SET materialize_statistics_on_insert = 0;

CREATE TABLE t_statistics_materialize
(
    a Int64 STATISTICS(tdigest),
    b Int16 STATISTICS(tdigest),
) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, enable_vertical_merge_algorithm = 0; -- TODO: there is a bug in vertical merge with statistics.

INSERT INTO t_statistics_materialize SELECT number, -number FROM system.numbers LIMIT 10000;

SELECT count(*) FROM t_statistics_materialize WHERE b < 10 and a < 10 SETTINGS log_comment = 'statistics not used';

OPTIMIZE TABLE t_statistics_materialize FINAL;

SELECT count(*) FROM t_statistics_materialize WHERE b < 10 and a < 10 SETTINGS log_comment = 'statistics used after merge';

TRUNCATE TABLE t_statistics_materialize;
SET mutations_sync = 2;

INSERT INTO t_statistics_materialize SELECT number, -number FROM system.numbers LIMIT 10000;
ALTER TABLE t_statistics_materialize MATERIALIZE STATISTICS a, b;

SELECT count(*) FROM t_statistics_materialize WHERE b < 10 and a < 10 SETTINGS log_comment = 'statistics used after materialize';

DROP TABLE t_statistics_materialize;

SYSTEM FLUSH LOGS;

SELECT log_comment, message FROM system.text_log JOIN
(
    SELECT Settings['log_comment'] AS log_comment, query_id FROM system.query_log
    WHERE current_database = currentDatabase()
        AND query LIKE 'SELECT count(*) FROM t_statistics_materialize%'
        AND type = 'QueryFinish'
) AS query_log USING (query_id)
WHERE message LIKE '%moved to PREWHERE%'
ORDER BY event_time_microseconds;

SELECT count(), sum(ProfileEvents['MergeTreeDataWriterStatisticsCalculationMicroseconds'])
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query LIKE 'INSERT INTO t_statistics_materialize SELECT%'
    AND type = 'QueryFinish';
