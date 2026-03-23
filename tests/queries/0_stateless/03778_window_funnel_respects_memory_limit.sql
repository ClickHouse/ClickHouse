-- Tags: no-random-mergetree-settings, no-random-settings, long

DROP TABLE IF EXISTS test;

CREATE TABLE test (ts Date, a String, b String, c String, d String) ENGINE= MergeTree() ORDER BY a;

SET max_rows_to_read = 0, max_insert_threads = 4, max_threads = 4;

INSERT INTO test SELECT today() - rand32()%25, toString(rand32()%25), toString(rand32()%25), toString(rand32()%25), toString(rand32()%25) FROM numbers_mt(1e8);

SELECT
    ifNull(fun_res, 0),
    count(*)
FROM
(
    -- getEventLevelStrictOnce <- AggregateFunctionWindowFunnel<T, Data>::insertResultInto
    -- will allocate more than 1GB for every list of ~4M events, which is more than the limit we set (800Mi)
    SELECT
        a,
        windowFunnel(86400000, 'strict_increase', 'strict_once')((b = '10') OR (c = '10') OR (b = '22') OR (c = '3') OR (b = '6') OR (c = '5') OR (b = '6') OR (c = '9'), (b = '9') OR (c = '91'), (b = '99') OR (c = '99'), (b = '74') OR (c = '74'), (b = '55') OR (c = '55'), (b = '13') OR (c = '13'), (b = '69') OR (c = '69')) AS fun_res
    FROM test
    GROUP BY a
)
GROUP BY fun_res
FORMAT Null
SETTINGS log_queries = 1, max_memory_usage = '800Mi'; -- { serverError MEMORY_LIMIT_EXCEEDED }

DROP TABLE test;
