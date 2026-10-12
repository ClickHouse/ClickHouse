-- This test genuinely inspects the literal `tag` recorded for cache entries, so the tags used by
-- the queries under test are prefixed with this file's own name to keep them from colliding with
-- tags used by other, concurrently running test files. `system.query_cache` is filtered to those
-- prefixed tags so that entries left behind by unrelated tests are not shown.
-- (Query parameters such as {CLICKHOUSE_TEST_UNIQUE_NAME:String} cannot be used here: they are
-- accepted only in expression positions such as a `WHERE` clause, not as the value of a
-- `SETTINGS`/`SET` clause or of `SYSTEM ... TAG`.)

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_tag';
SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_tag_abc';
SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_tag_def';

-- Store the result a single query with a tag in the query cache and check that the system table knows about the tag
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = '02494_query_cache_tag_abc';

SELECT query, tag FROM (SELECT * FROM system.query_cache WHERE tag LIKE '02494_query_cache_tag%') AS test_query_cache;

SELECT '---';

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_tag';
SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_tag_abc';
SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_tag_def';

-- Store the result of the same query with two different tags. The cache should store two entries.
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = '02494_query_cache_tag'; -- stands in for the default query_cache_tag = ''
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = '02494_query_cache_tag_abc';
SELECT query, tag FROM (SELECT * FROM system.query_cache WHERE tag LIKE '02494_query_cache_tag%') AS test_query_cache ORDER BY ALL;

SELECT '---';

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_tag';
SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_tag_abc';
SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_tag_def';

-- Like before but the tag is set standalone.

SET query_cache_tag = '02494_query_cache_tag_abc';
SELECT 1 SETTINGS use_query_cache = true;

SET query_cache_tag = '02494_query_cache_tag_def';
SELECT 1 SETTINGS use_query_cache = true;

SELECT query, tag FROM (SELECT * FROM system.query_cache WHERE tag LIKE '02494_query_cache_tag%') AS test_query_cache ORDER BY ALL;

SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_tag';
SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_tag_abc';
SYSTEM CLEAR QUERY CACHE TAG '02494_query_cache_tag_def';
