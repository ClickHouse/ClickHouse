#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The query cache is server-global, so every section tags its entries with the unique database
# name and counts only its own rows. That keeps the test safe to run in parallel with itself and
# removes any need to clear the cache.

# The cache settings are set for the session rather than per query: a query-level SETTINGS clause
# attaches to the inner SELECT normally but to the outer node when an output suffix is present,
# which would differentiate the ASTs being compared here for an unrelated reason.
cache_on() {
    echo "SET use_query_cache = 1, enable_writes_to_query_cache = 1, enable_reads_from_query_cache = 1,
              query_cache_min_query_runs = 0, query_cache_min_query_duration = 0,
              query_cache_tag = '${CLICKHOUSE_DATABASE}$1';"
}

# Counting reads a system table, so it has to stay out of the cache itself.
entries() { ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.query_cache WHERE tag = '${CLICKHOUSE_DATABASE}$1' SETTINGS use_query_cache = 0"; }

echo 'INTO OUTFILE modifiers do not reach the key'
# The cache stores result blocks, so the whole output suffix is removed before hashing. The
# APPEND / TRUNCATE / AND STDOUT modifiers are not children, so they need clearing separately.
${CLICKHOUSE_CLIENT} --multiquery --query "$(cache_on _outfile)
SELECT 858;
SELECT 858 INTO OUTFILE '${CLICKHOUSE_TMP}/04858_a.tsv' APPEND FORMAT TSV;
SELECT 858 INTO OUTFILE '${CLICKHOUSE_TMP}/04858_a.tsv' TRUNCATE FORMAT TSV;
" > /dev/null
entries _outfile

echo 'settings reset with DEFAULT do not reach the key'
# Settings ignored by the cache are erased from the AST key. `x = DEFAULT` is held in
# ASTSetQuery::default_settings rather than in changes, so it has to be erased from there too.
${CLICKHOUSE_CLIENT} --multiquery --query "$(cache_on _default)
SELECT 858 SETTINGS max_block_size = 1234;
SELECT 858 SETTINGS max_block_size = 1234, query_cache_ttl = DEFAULT;
" > /dev/null
entries _default

echo 'a setting that does affect the result still separates keys'
${CLICKHOUSE_CLIENT} --multiquery --query "$(cache_on _control)
SELECT 858 SETTINGS max_block_size = 1234;
SELECT 858 SETTINGS max_block_size = 4321;
" > /dev/null
entries _control

echo 'UNION ALL nesting does not reach the key'
# Both queries normalize to the same flat UNION ALL chain and format to the same SQL, so
# list_of_modes must not be hashed once is_normalized is set.
${CLICKHOUSE_CLIENT} --multiquery --query "$(cache_on _union)
SELECT sum(x) FROM (SELECT 1 AS x UNION ALL SELECT 2 UNION ALL SELECT 3);
SELECT sum(x) FROM (SELECT 1 AS x UNION ALL (SELECT 2 UNION ALL SELECT 3));
" > /dev/null
entries _union

echo 'a different set operation still separates keys'
${CLICKHOUSE_CLIENT} --multiquery --query "$(cache_on _union_control)
SELECT sum(x) FROM (SELECT 1 AS x UNION ALL SELECT 1);
SELECT sum(x) FROM (SELECT 1 AS x UNION DISTINCT SELECT 1);
" > /dev/null
entries _union_control
