#!/usr/bin/env bash

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

for suffix in a b; do
    printf '{"x":[[[[[[[[1]]]]]]]]}\n' > "${T}_deep_${suffix}.json"
done
touch -d "$AGE" "${T}"_*

# --- max_parser_depth --------------------------------------------------------------------
# A low limit must keep throwing after a high-limit query warmed the cache.
echo "-- JSONEachRow depth, high limit then low limit must throw"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_deep_a.json', 'JSONEachRow') SETTINGS max_parser_depth = 1000 FORMAT Null;
    DESC file('${T}_deep_a.json', 'JSONEachRow') SETTINGS max_parser_depth = 3 FORMAT Null;" \
    2>&1 | grep -c TOO_DEEP_RECURSION
echo "-- JSONEachRow depth, low limit alone throws (control)"
$CLICKHOUSE_LOCAL -q "DESC file('${T}_deep_b.json', 'JSONEachRow') SETTINGS max_parser_depth = 3 FORMAT Null" \
    2>&1 | grep -c TOO_DEEP_RECURSION

rm -f "${T}"_*
