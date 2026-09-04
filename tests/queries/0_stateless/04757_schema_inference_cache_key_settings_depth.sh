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

for suffix in a b; do
    printf '{"x":[[[[[[[[1]]]]]]]]}\n' > "${T}_deep_${suffix}.json"
    printf '[[[[[[1]]]]]]\n' > "${T}_deep_${suffix}.tsv"
done
$CLICKHOUSE_LOCAL -q "SELECT [[[[[[toUInt32(1)]]]]]] AS x INTO OUTFILE '${T}_deep_a.parquet' TRUNCATE FORMAT Parquet"
cp "${T}_deep_a.parquet" "${T}_deep_b.parquet"
touch -d "$AGE" "${T}"_*

# --- max_parser_depth --------------------------------------------------------------------
# A low limit must keep throwing after a high-limit query warmed the cache.
for fmt_file in "JSONEachRow json" "TSV tsv"; do
    set -- $fmt_file
    echo "-- $1 depth, high limit then low limit must throw"
    $CLICKHOUSE_LOCAL -m -q "
        DESC file('${T}_deep_a.$2', '$1') SETTINGS max_parser_depth = 1000 FORMAT Null;
        DESC file('${T}_deep_a.$2', '$1') SETTINGS max_parser_depth = 3 FORMAT Null;" \
        2>&1 | grep -c TOO_DEEP_RECURSION
    echo "-- $1 depth, low limit alone throws (control)"
    $CLICKHOUSE_LOCAL -q "DESC file('${T}_deep_b.$2', '$1') SETTINGS max_parser_depth = 3 FORMAT Null" \
        2>&1 | grep -c TOO_DEEP_RECURSION
done

echo "-- Parquet depth, high limit then low limit must throw"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_deep_a.parquet', 'Parquet') SETTINGS max_parser_depth = 1000 FORMAT Null;
    DESC file('${T}_deep_a.parquet', 'Parquet') SETTINGS max_parser_depth = 2 FORMAT Null;" \
    2>&1 | grep -c TOO_DEEP_RECURSION
echo "-- Parquet depth, low limit alone throws (control)"
$CLICKHOUSE_LOCAL -q "DESC file('${T}_deep_b.parquet', 'Parquet') SETTINGS max_parser_depth = 2 FORMAT Null" \
    2>&1 | grep -c TOO_DEEP_RECURSION

# --- input_format_parquet_local_time_as_utc -----------------------------------------------
# Selects the timezone of the inferred DateTime64 for a non-UTC-adjusted timestamp column,
# so each pair must report DateTime64(3, 'UTC') for the =1 query and DateTime64(3) for the =0
# query, whichever ran first. A stale entry also changes the value read back, not just the name.
for suffix in a b; do cp "$CUR_DIR"/data_parquet/not_utc.parquet "${T}_lt_${suffix}.parquet"; done
touch -d "$AGE" "${T}"_lt_*.parquet
echo "-- Parquet local_time_as_utc, utc=1 first"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_lt_a.parquet', 'Parquet') SETTINGS input_format_parquet_local_time_as_utc = 1;
    DESC file('${T}_lt_a.parquet', 'Parquet') SETTINGS input_format_parquet_local_time_as_utc = 0;" | cut -f2
echo "-- Parquet local_time_as_utc, utc=0 first"
$CLICKHOUSE_LOCAL -m -q "
    DESC file('${T}_lt_b.parquet', 'Parquet') SETTINGS input_format_parquet_local_time_as_utc = 0;
    DESC file('${T}_lt_b.parquet', 'Parquet') SETTINGS input_format_parquet_local_time_as_utc = 1;" | cut -f2

rm -f "${T}"_*
