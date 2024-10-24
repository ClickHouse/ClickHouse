-- Tags: no-parallel

set log_queries=1;
set log_query_threads=1;
set max_threads=0;
set use_concurrency_control=0;

WITH 01091 AS id SELECT 1;
SYSTEM FLUSH LOGS;

WITH
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase() AND (normalizeQuery(query) like normalizeQuery('WITH 01091 AS id SELECT 1;')) AND (event_date >= (today() - 1))
        ORDER BY event_time DESC
        LIMIT 1
    ) AS id
SELECT uniqExact(thread_id)
FROM system.query_thread_log
WHERE (event_date >= (today() - 1)) AND (query_id = id) AND (thread_id != master_thread_id);

with 01091 as id select sum(number) from numbers(1000000);
SYSTEM FLUSH LOGS;

WITH
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase() AND (normalizeQuery(query) = normalizeQuery('with 01091 as id select sum(number) from numbers(1000000);')) AND (event_date >= (today() - 1))
        ORDER BY event_time DESC
        LIMIT 1
    ) AS id
SELECT uniqExact(thread_id) > 2
FROM system.query_thread_log
WHERE (event_date >= (today() - 1)) AND (query_id = id) AND (thread_id != master_thread_id);

with 01091 as id select sum(number) from numbers_mt(1000000);
SYSTEM FLUSH LOGS;

WITH
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase() AND (normalizeQuery(query) = normalizeQuery('with 01091 as id select sum(number) from numbers_mt(1000000);')) AND (event_date >= (today() - 1))
        ORDER BY event_time DESC
        LIMIT 1
    ) AS id
SELECT uniqExact(thread_id) > 2
FROM system.query_thread_log
WHERE (event_date >= (today() - 1)) AND (query_id = id) AND (thread_id != master_thread_id);
