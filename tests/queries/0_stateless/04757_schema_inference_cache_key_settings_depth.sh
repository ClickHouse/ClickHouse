#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs Parquet.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Each carrier below is probed in BOTH orders, because an order-insensitive probe cannot
# detect a missing cache-key field: whichever query runs first decides the cached type.
# Every order gets its OWN file so the two orders never share a cache entry.
#
# The fixtures are aged with `touch -d`: SchemaCache::tryGetImpl drops an entry when the
# source's mtime is >= the entry's registration time, and both are whole seconds, so a file
# written in the same second as the first query is re-inferred and nothing is cached.

T="${CLICKHOUSE_TEST_UNIQUE_NAME}"
AGE="2000-01-01 00:00:00"

$CLICKHOUSE_LOCAL -q "SELECT [[[[[[toUInt32(1)]]]]]] AS x INTO OUTFILE '${T}_deep_a.parquet' TRUNCATE FORMAT Parquet"
cp "${T}_deep_a.parquet" "${T}_deep_b.parquet"
touch -d "$AGE" "${T}"_*

# --- max_parser_depth --------------------------------------------------------------------
# A low limit must keep throwing after a high-limit query warmed the cache.
echo "-- Parquet depth, high limit then low limit must throw"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_deep_a.parquet', 'Parquet') SETTINGS max_parser_depth = 1000 FORMAT Null;
    DESC file('${T}_deep_a.parquet', 'Parquet') SETTINGS max_parser_depth = 2 FORMAT Null;" \
    2>&1 | grep -c TOO_DEEP_RECURSION
echo "-- Parquet depth, low limit alone throws (control)"
$CLICKHOUSE_LOCAL -q "DESC file('${T}_deep_b.parquet', 'Parquet') SETTINGS max_parser_depth = 2 FORMAT Null" \
    2>&1 | grep -c TOO_DEEP_RECURSION

rm -f "${T}"_*
