DROP TABLE IF EXISTS t_statistic_materialize;

SET allow_experimental_statistic = 1;
SET allow_statistic_optimize = 1;
SET materialize_statistics_on_insert = 0;

CREATE TABLE t_statistic_materialize
(
    a Int64 STATISTIC(tdigest),
    b Int16 STATISTIC(tdigest),
) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, enable_vertical_merge_algorithm = 0; -- TODO: there is a bug in vertical merge with statistics.

INSERT INTO t_statistic_materialize SELECT number, -number FROM system.numbers LIMIT 10000;

SELECT count(*) FROM t_statistic_materialize WHERE b < 10 and a < 10 SETTINGS log_comment = 'statistic not used';

OPTIMIZE TABLE t_statistic_materialize FINAL;

SELECT count(*) FROM t_statistic_materialize WHERE b < 10 and a < 10 SETTINGS log_comment = 'statistic used after merge';

TRUNCATE TABLE t_statistic_materialize;
SET mutations_sync = 2;

INSERT INTO t_statistic_materialize SELECT number, -number FROM system.numbers LIMIT 10000;
ALTER TABLE t_statistic_materialize MATERIALIZE STATISTIC a, b TYPE tdigest;

SELECT count(*) FROM t_statistic_materialize WHERE b < 10 and a < 10 SETTINGS log_comment = 'statistic used after materialize';

DROP TABLE t_statistic_materialize;

SYSTEM FLUSH LOGS;

SELECT log_comment, message FROM system.text_log JOIN
(
    SELECT Settings['log_comment'] AS log_comment, query_id FROM system.query_log
    WHERE current_database = currentDatabase()
        AND query LIKE 'SELECT count(*) FROM t_statistic_materialize%'
        AND type = 'QueryFinish'
) AS query_log USING (query_id)
WHERE message LIKE '%moved to PREWHERE%'
ORDER BY event_time_microseconds;

SELECT count(), sum(ProfileEvents['MergeTreeDataWriterStatisticsCalculationMicroseconds'])
FROM system.query_log
WHERE current_database = currentDatabase()
    AND query LIKE 'INSERT INTO t_statistic_materialize SELECT%'
    AND type = 'QueryFinish';
