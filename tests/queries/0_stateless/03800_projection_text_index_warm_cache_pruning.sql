-- Tags: no-parallel-replicas
-- Regression test: granule pruning must not depend on the text-index tokens cache.
--
-- `deserializeBinaryWithMultipleStreams` returns early when every search token was
-- served from `TextIndexTokensCache`. That early return used to skip opening the
-- `.pst` stream, which `ProjectionTokenInfo::hasDocInRange` needs to decode a packed
-- block precisely. Without the stream it conservatively answered "may contain" for
-- every mark, so the first query on a part pruned correctly and every later one read
-- all granules.

SET allow_experimental_projection_text_index = 1;
-- The reference uses the pre-26.7 EXPLAIN layout; explain_query_plan_default now defaults to 'pretty'.
SET explain_query_plan_default = 'legacy';
SET enable_full_text_index = 1;

DROP TABLE IF EXISTS t_warm_cache_pruning;

CREATE TABLE t_warm_cache_pruning
(
    id UInt32,
    message String,
    PROJECTION idx INDEX message TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

-- 'World' occurs in exactly 1 of every 4 rows, so only 256 of 1024 granules match.
INSERT INTO t_warm_cache_pruning
SELECT
    number,
    CASE
        WHEN modulo(number, 4) = 0 THEN 'Hello, ClickHouse'
        WHEN modulo(number, 4) = 1 THEN 'Hello, World'
        WHEN modulo(number, 4) = 2 THEN 'Hallo, ClickHouse'
        WHEN modulo(number, 4) = 3 THEN 'ClickHouse is the fast, really fast!'
    END
FROM numbers(1024);

SYSTEM DROP TEXT INDEX CACHES;

-- Cold: nothing is cached yet, the token is read from the dictionary.
SELECT 'cold';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM t_warm_cache_pruning WHERE hasToken(message, 'World')
)
WHERE explain LIKE '%Granules: %/%' AND explain NOT LIKE '%1024/1024%';

-- Warm: the same token now comes from TextIndexTokensCache. Pruning must be identical.
SELECT 'warm';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM t_warm_cache_pruning WHERE hasToken(message, 'World')
)
WHERE explain LIKE '%Granules: %/%' AND explain NOT LIKE '%1024/1024%';

-- Results must stay correct on both paths.
SELECT 'count', count() FROM t_warm_cache_pruning WHERE hasToken(message, 'World');

DROP TABLE t_warm_cache_pruning;
