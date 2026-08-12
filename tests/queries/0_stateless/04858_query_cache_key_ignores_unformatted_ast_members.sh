#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The query cache is server-global, so every section tags its entries with the unique database
# name and counts only its own rows. That keeps the test safe to run in parallel with itself and
# removes any need to clear the cache.

QS="use_query_cache = 1, enable_writes_to_query_cache = 1, enable_reads_from_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0"

run() { ${CLICKHOUSE_CLIENT} --query "$1 SETTINGS $QS, query_cache_tag = '${CLICKHOUSE_DATABASE}$2'" >/dev/null; }

# Counting reads a system table, so it has to stay out of the cache itself.
entries() { ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.query_cache WHERE tag = '${CLICKHOUSE_DATABASE}$1' SETTINGS use_query_cache = 0"; }

echo 'settings reset with DEFAULT do not reach the key'
# Settings ignored by the cache are erased from the AST key. `x = DEFAULT` is held in
# ASTSetQuery::default_settings rather than in changes, so it has to be erased from there too.
run "SELECT 858 SETTINGS max_block_size = 1234" _default
run "SELECT 858 SETTINGS max_block_size = 1234, query_cache_ttl = DEFAULT" _default
entries _default

echo 'a setting that does affect the result still separates keys'
run "SELECT 858 SETTINGS max_block_size = 1234" _control
run "SELECT 858 SETTINGS max_block_size = 4321" _control
entries _control

echo 'UNION ALL nesting does not reach the key'
# Both queries normalize to the same flat UNION ALL chain and format to the same SQL, so
# list_of_modes must not be hashed once is_normalized is set.
run "SELECT sum(x) FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3)" _union
run "SELECT sum(x) FROM (SELECT 1 AS x UNION ALL (SELECT 2 UNION ALL SELECT 3))" _union
entries _union

echo 'a different set operation still separates keys'
run "SELECT sum(x) FROM (SELECT 1 AS x UNION ALL SELECT 1)" _union_control
run "SELECT sum(x) FROM (SELECT 1 AS x UNION DISTINCT SELECT 1)" _union_control
entries _union_control
