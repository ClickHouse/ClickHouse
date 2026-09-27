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
