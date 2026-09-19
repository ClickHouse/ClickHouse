#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An inferred `DateTime` without an explicit time zone latches the effective time zone of the
# session that inferred it (`DateLUT::instance` resolves `session_timezone`), and the schema
# inference cache stores the type objects themselves. A session with another `session_timezone`
# must not be served that type as is: it would parse the values in its own zone but format and
# compute them in the inferring session's zone. The cache re-binds an implicit zone to the
# reading session's zone on every hit.
#
# Every probe runs in BOTH orders, because an order-insensitive probe cannot detect a missing
# re-bind: whichever query runs first decides the cached type. Every order gets its own file so
# the two orders never share a cache entry.
#
# The fixtures are aged with `touch -d`: `SchemaCache::tryGetImpl` drops an entry when the
# source's mtime is >= the entry's registration time, and both are whole seconds, so a file
# written in the same second as the first query is re-inferred and nothing is cached.

T="${CLICKHOUSE_TEST_UNIQUE_NAME}"
AGE="2000-01-01 00:00:00"

for suffix in a b c d; do
    printf '2020-06-01 12:00:00\n' > "${T}_${suffix}.csv"
done
for suffix in e f; do
    printf '{"arr":["2020-06-01 12:00:00"],"tup":{"k":"2020-06-01 12:00:00"},"sub":"2020-06-01 12:00:00.123"}\n' > "${T}_${suffix}.jsonl"
done
$CLICKHOUSE_LOCAL -q "SELECT toDateTime64('2020-06-01 12:00:00.123', 3, 'UTC') AS t FORMAT RowBinaryWithNamesAndTypes" > "${T}_g.bin"
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

# The time zone is not part of the cache key: sessions with different `session_timezone` keep
# sharing one entry per source (and the cached number of rows with it), and the re-bound type
# is served from that single entry without re-reading the file.
echo "-- one entry shared by all time zones"
$CLICKHOUSE_LOCAL -m -q "
    SELECT c1 FROM file('${T}_d.csv') SETTINGS session_timezone = 'Asia/Tokyo' FORMAT Null;
    SELECT c1 FROM file('${T}_d.csv') SETTINGS session_timezone = 'Europe/Berlin' FORMAT Null;
    SELECT c1 FROM file('${T}_d.csv') FORMAT Null;
    SELECT count(), countDistinct(additional_format_info) FROM system.schema_inference_cache;
    SELECT timeZoneOf(c1) FROM file('${T}_d.csv') SETTINGS session_timezone = 'America/New_York';
    SELECT count() FROM system.schema_inference_cache;"

# A `DateTime` nested in an `Array` or a `Tuple` and the scale of a `DateTime64` go through the
# same re-bind. `SchemaInferenceCacheSchemaHits` is asserted with them: re-inferring the file in
# the reading session's own zone prints the very same output, so without that counter the probe
# cannot tell a re-bound cached type from a cache that was never consulted.
echo "-- a nested time zone and a scale, Tokyo first"
$CLICKHOUSE_LOCAL -m -q "
    SELECT timeZoneOf(arr[1]), timeZoneOf(tup.k), timeZoneOf(sub), toTypeName(sub) FROM file('${T}_e.jsonl', JSONEachRow) SETTINGS session_timezone = 'Asia/Tokyo';
    SELECT timeZoneOf(arr[1]), timeZoneOf(tup.k), timeZoneOf(sub), toTypeName(sub) FROM file('${T}_e.jsonl', JSONEachRow) SETTINGS session_timezone = 'Europe/Berlin';
    SELECT sum(value) > 0 FROM system.events WHERE event = 'SchemaInferenceCacheSchemaHits';"

echo "-- a nested time zone and a scale, Berlin first"
$CLICKHOUSE_LOCAL -m -q "
    SELECT timeZoneOf(arr[1]), timeZoneOf(tup.k), timeZoneOf(sub), toTypeName(sub) FROM file('${T}_f.jsonl', JSONEachRow) SETTINGS session_timezone = 'Europe/Berlin';
    SELECT timeZoneOf(arr[1]), timeZoneOf(tup.k), timeZoneOf(sub), toTypeName(sub) FROM file('${T}_f.jsonl', JSONEachRow) SETTINGS session_timezone = 'Asia/Tokyo';
    SELECT sum(value) > 0 FROM system.events WHERE event = 'SchemaInferenceCacheSchemaHits';"

# The other half: a type that carries an explicit time zone keeps it, it is not replaced by the
# reading session's zone.
echo "-- an explicit time zone survives the re-bind"
$CLICKHOUSE_LOCAL -m -q "
    SELECT timeZoneOf(t), toString(t) FROM file('${T}_g.bin', RowBinaryWithNamesAndTypes) SETTINGS session_timezone = 'Asia/Tokyo';
    SELECT timeZoneOf(t), toString(t) FROM file('${T}_g.bin', RowBinaryWithNamesAndTypes) SETTINGS session_timezone = 'Europe/Berlin';
    SELECT sum(value) > 0 FROM system.events WHERE event = 'SchemaInferenceCacheSchemaHits';"

rm -f "${T}"_*
