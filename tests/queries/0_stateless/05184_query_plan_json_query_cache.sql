-- Tags: no-old-analyzer, no-parallel-replicas

-- A query answered from the query result cache runs no pipeline, so it has no plan to store. That
-- is correct, but it happens above `canEnableProfiler` -- the whole interpreter branch is skipped
-- on a hit -- so the reason has to be reported from there instead, or the column is just silently
-- empty for a query that asked for a plan.
--
-- Both statements are identical and the settings are set outside them, because the cache key is a
-- hash of the AST and the settings: a `SETTINGS log_comment = ...` clause would make the second
-- query a different key and it would never hit. The two rows are told apart by `query_cache_usage`.

SET log_query_plans = 1;
SET use_query_cache = 1;
SET log_comment = '05184_query_cache';

SELECT sum(number) FROM numbers(1000) FORMAT Null;
SELECT sum(number) FROM numbers(1000) FORMAT Null;

SET use_query_cache = 0;
SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    'query_cache',
    -- One of each: the first execution filled the cache, the second was served from it.
    countIf(query_cache_usage = 'Write'),
    countIf(query_cache_usage = 'Read'),
    -- The one that executed has a plan.
    countIf(query_cache_usage = 'Write' AND length(JSONExtractArrayRaw(toJSONString(query_plan), 'Nodes')) > 0),
    -- The one served from the cache has none, because nothing ran.
    countIf(query_cache_usage = 'Read' AND empty(JSONExtractArrayRaw(toJSONString(query_plan), 'Nodes')))
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05184_query_cache';
