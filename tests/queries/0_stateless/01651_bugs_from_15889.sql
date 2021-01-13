DROP TABLE IF EXISTS xp;
DROP TABLE IF EXISTS xp_d;

CREATE TABLE xp (`A` Date, `B` Int64, `S` String) ENGINE = MergeTree PARTITION BY toYYYYMM(A) ORDER BY B;
INSERT INTO xp SELECT '2020-01-01', number, '' FROM numbers(100000);

CREATE TABLE xp_d AS xp ENGINE = Distributed(test_shard_localhost, currentDatabase(), xp);

SELECT count(7 = (SELECT number FROM numbers(0) ORDER BY number ASC NULLS FIRST LIMIT 7)) FROM xp_d PREWHERE toYYYYMM(A) GLOBAL IN (SELECT NULL = (SELECT number FROM numbers(1) ORDER BY number DESC NULLS LAST LIMIT 1), toYYYYMM(min(A)) FROM xp_d) WHERE B > NULL; -- { serverError 20 }

SELECT count() FROM xp_d WHERE A GLOBAL IN (SELECT NULL); -- { serverError 53 }

DROP TABLE IF EXISTS xp;
DROP TABLE IF EXISTS xp_d;

set allow_introspection_functions = 1;

WITH concat(addressToLine(arrayJoin(trace) AS addr), '#') AS symbol
SELECT count() > 7
FROM system.trace_log AS t
WHERE (query_id = 
(
    SELECT
        [NULL, NULL, NULL, NULL, 0.00009999999747378752, NULL, NULL, NULL, NULL, NULL],
        query_id
    FROM system.query_log
    WHERE (query LIKE '%test cpu time query profiler%') AND (query NOT LIKE '%system%')
    ORDER BY event_time DESC
    LIMIT 1
)) AND (symbol LIKE '%Source%');


WITH addressToSymbol(arrayJoin(trace)) AS symbol
SELECT count() > 0
FROM system.trace_log AS t
WHERE greaterOrEquals(event_date, ignore(ignore(ignore(NULL, '')), 256), yesterday()) AND (trace_type = 'Memory') AND (query_id = 
(
    SELECT
        ignore(ignore(ignore(ignore(65536)), ignore(65537), ignore(2)), ''),
        query_id
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (query LIKE '%test memory profiler%')
    ORDER BY event_time DESC
    LIMIT 1
)); -- { serverError 42 }

WITH (
    (
        SELECT query_start_time_microseconds
        FROM system.query_log
        ORDER BY query_start_time DESC
        LIMIT 1
    ) AS time_with_microseconds, 
    (
        SELECT
            inf,
            query_start_time
        FROM system.query_log
        ORDER BY query_start_time DESC
        LIMIT 1
    ) AS t)
SELECT if(dateDiff('second', toDateTime(time_with_microseconds), toDateTime(t)) = -9223372036854775808, 'ok', ''); -- { serverError 43 }

WITH (
    (
        SELECT query_start_time_microseconds
        FROM system.query_log
        ORDER BY query_start_time DESC
        LIMIT 1
    ) AS time_with_microseconds, 
    (
        SELECT query_start_time
        FROM system.query_log
        ORDER BY query_start_time DESC
        LIMIT 1
    ) AS t)
SELECT if(dateDiff('second', toDateTime(time_with_microseconds), toDateTime(t)) = -9223372036854775808, 'ok', '');




