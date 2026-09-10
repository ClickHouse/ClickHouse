#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An inferred `DateTime` without an explicit time zone latches the effective time zone of the
# session that inferred it (`DateLUT::instance` resolves `session_timezone`), and the schema
# inference cache stores the type objects themselves. A session with another `session_timezone`
# must not be served that schema: it would parse the values in its own zone but format and
# compute them in the inferring session's zone.
#
# Every probe runs in BOTH orders, because an order-insensitive probe cannot detect a missing
# cache-key field: whichever query runs first decides the cached type. Every order gets its own
# file so the two orders never share a cache entry.
#
# The fixtures are aged with `touch -d`: `SchemaCache::tryGetImpl` drops an entry when the
# source's mtime is >= the entry's registration time, and both are whole seconds, so a file
# written in the same second as the first query is re-inferred and nothing is cached.

T="${CLICKHOUSE_TEST_UNIQUE_NAME}"
AGE="2000-01-01 00:00:00"

for suffix in a b c d; do
    printf '2020-06-01 12:00:00\n' > "${T}_${suffix}.csv"
done
touch -d "$AGE" "${T}"_*

echo "-- the time zone of the type, Tokyo first"
$CLICKHOUSE_LOCAL -m -q "
    SELECT timeZoneOf(c1) FROM file('${T}_a.csv') SETTINGS session_timezone = 'Asia/Tokyo';
    SELECT timeZoneOf(c1) FROM file('${T}_a.csv') SETTINGS session_timezone = 'Europe/Berlin';"

echo "-- the time zone of the type, Berlin first"
$CLICKHOUSE_LOCAL -m -q "
    SELECT timeZoneOf(c1) FROM file('${T}_b.csv') SETTINGS session_timezone = 'Europe/Berlin';
    SELECT timeZoneOf(c1) FROM file('${T}_b.csv') SETTINGS session_timezone = 'Asia/Tokyo';"

# The value channel, not just the type: the same wall clock text is a different point in time
# in each zone, and reading it in the other session's zone shifts `toUnixTimestamp` by 7 hours.
echo "-- the values, Tokyo first"
$CLICKHOUSE_LOCAL -m -q "
    SELECT toUnixTimestamp(c1), toHour(c1) FROM file('${T}_c.csv') SETTINGS session_timezone = 'Asia/Tokyo';
    SELECT toUnixTimestamp(c1), toHour(c1) FROM file('${T}_c.csv') SETTINGS session_timezone = 'Europe/Berlin';"

# Two sessions with the same `session_timezone` must keep sharing one entry, and a session that
# does not set it must not get an entry of its own: the key of a default session is unchanged.
echo "-- one entry per distinct time zone"
$CLICKHOUSE_LOCAL -m -q "
    SELECT c1 FROM file('${T}_d.csv') SETTINGS session_timezone = 'Asia/Tokyo' FORMAT Null;
    SELECT c1 FROM file('${T}_d.csv') SETTINGS session_timezone = 'Asia/Tokyo' FORMAT Null;
    SELECT c1 FROM file('${T}_d.csv') FORMAT Null;
    SELECT c1 FROM file('${T}_d.csv') FORMAT Null;
    SELECT count(), countDistinct(additional_format_info) FROM system.schema_inference_cache;"

rm -f "${T}"_*
