#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A per-run query cache tag: a literal tag would be shared by concurrent copies of this test (flaky check),
# whose tag-scoped clears would then wipe each other's entries. Query parameters cannot be used in
# `SET` values or `SYSTEM ... TAG` arguments, hence the shell variable.
TAG="${CLICKHOUSE_DATABASE}_03381"

$CLICKHOUSE_CLIENT --multiquery "
SET query_cache_tag = '${TAG}';

SET enable_analyzer = 1;

SYSTEM CLEAR QUERY CACHE TAG '${TAG}';

-- Test 1: CTE with query_cache_for_subqueries creates cache entry
WITH sq AS (SELECT sum(number) AS s FROM numbers(100))
SELECT s FROM sq
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

SELECT count(*) > 0 FROM (SELECT * FROM system.query_cache WHERE tag = '${TAG}') AS test_query_cache WHERE is_subquery = 1;
-- Expected: 1 (true)

SYSTEM CLEAR QUERY CACHE TAG '${TAG}';

-- Test 2: CTE cache hit on second run
WITH sq AS (SELECT sum(number) AS s FROM numbers(100))
SELECT s FROM sq
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

WITH sq AS (SELECT sum(number) AS s FROM numbers(100))
SELECT s FROM sq
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

SELECT count(*) > 0 FROM (SELECT * FROM system.query_cache WHERE tag = '${TAG}') AS test_query_cache WHERE is_subquery = 1;
-- Expected: 1 (true, cache entry exists)

SYSTEM CLEAR QUERY CACHE TAG '${TAG}';

-- Test 3: Explicit opt-in on CTE subquery without query_cache_for_subqueries
SELECT s FROM (SELECT sum(number) AS s FROM numbers(100) SETTINGS use_query_cache = true);

SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag = '${TAG}') AS test_query_cache WHERE is_subquery = 1;
-- Expected: 1

SYSTEM CLEAR QUERY CACHE TAG '${TAG}';
"
