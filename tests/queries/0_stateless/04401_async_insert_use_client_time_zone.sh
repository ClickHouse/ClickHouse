#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings
# ^ no-random-settings: the runner must not inject a randomized `session_timezone`; an explicit
#   `session_timezone` (even empty) is an intentional user override and disables the client-time-zone
#   propagation this test exercises.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# With use_client_time_zone=1, a DateTime string literal must be interpreted in the client time zone
# regardless of whether the INSERT is synchronous or asynchronous. The async path parses the VALUES
# block on the server, so the client has to propagate its local time zone as session_timezone, and it
# must do so for every query (not only at connect time), tracking use_client_time_zone changes in both
# directions. America/Hermosillo is a fixed UTC-7 zone (no DST), so 2017-07-14 05:40:00 there is the
# stable instant 1500036000.

TZC="env TZ=America/Hermosillo ${CLICKHOUSE_CLIENT}"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.dt (a DateTime, kind String) ENGINE = Memory"

# use_client_time_zone via the command line flag (async and sync).
$TZC --use_client_time_zone=1 -q \
  "INSERT INTO ${CLICKHOUSE_DATABASE}.dt SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES ('2017-07-14 05:40:00', 'flag_async')"
$TZC --use_client_time_zone=1 -q \
  "INSERT INTO ${CLICKHOUSE_DATABASE}.dt SETTINGS async_insert = 0 VALUES ('2017-07-14 05:40:00', 'flag_sync')"

# use_client_time_zone turned on mid-session with SET, on an already-open connection.
$TZC -mn -q "
SET use_client_time_zone = 1;
INSERT INTO ${CLICKHOUSE_DATABASE}.dt SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES ('2017-07-14 05:40:00', 'set_async');
INSERT INTO ${CLICKHOUSE_DATABASE}.dt SETTINGS async_insert = 0 VALUES ('2017-07-14 05:40:00', 'set_sync');
"

# use_client_time_zone set only per query via SETTINGS, without the command line flag.
$TZC -q \
  "INSERT INTO ${CLICKHOUSE_DATABASE}.dt SETTINGS use_client_time_zone = 1, async_insert = 1, wait_for_async_insert = 1 VALUES ('2017-07-14 05:40:00', 'settings_async')"

# Starting with an explicit --session_timezone override and then clearing it with
# SET session_timezone = DEFAULT must fall back to the client time zone, not to the override value.
$TZC --use_client_time_zone=1 --session_timezone=UTC -mn -q "
SET session_timezone = DEFAULT;
INSERT INTO ${CLICKHOUSE_DATABASE}.dt SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES ('2017-07-14 05:40:00', 'reset_default_async');
"

# All of the above interpret the literal in the client time zone: 2017-07-14 05:40:00 = 1500036000.
${CLICKHOUSE_CLIENT} -q "SELECT kind, toUnixTimestamp(a) FROM ${CLICKHOUSE_DATABASE}.dt ORDER BY kind"

# Turning use_client_time_zone back off must restore server-side parsing (the stored instant no longer
# depends on the client time zone). The exact value depends on the server time zone, so compare it with
# a plain default insert instead of hard-coding it.
$TZC -q \
  "INSERT INTO ${CLICKHOUSE_DATABASE}.dt SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES ('2017-07-14 05:40:00', 'server_ref')"
$TZC --use_client_time_zone=1 -mn -q "
SET use_client_time_zone = 0;
INSERT INTO ${CLICKHOUSE_DATABASE}.dt SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES ('2017-07-14 05:40:00', 'reset_async');
"
${CLICKHOUSE_CLIENT} -q "
SELECT 'reset_async matches server tz', (
    (SELECT toUnixTimestamp(a) FROM ${CLICKHOUSE_DATABASE}.dt WHERE kind = 'reset_async')
    = (SELECT toUnixTimestamp(a) FROM ${CLICKHOUSE_DATABASE}.dt WHERE kind = 'server_ref'))
"

server_output=$(${CLICKHOUSE_CLIENT} --use_client_time_zone=0 -q "
SELECT toString(toDateTime(1500036000)), toString(fromUnixTimestamp64Milli(toInt64(1500036000125)))
")
expected_output=$(printf '%s\n%s\t%s\n%s\n' "$server_output" '2017-07-14 05:40:00' '2017-07-14 05:40:00.125' "$server_output")
actual_output=$($TZC --use_client_time_zone=1 -mn -q "
SELECT toDateTime(1500036000), fromUnixTimestamp64Milli(toInt64(1500036000125)) SETTINGS use_client_time_zone = 0;
SELECT toDateTime(1500036000), fromUnixTimestamp64Milli(toInt64(1500036000125));
SELECT toDateTime(1500036000), fromUnixTimestamp64Milli(toInt64(1500036000125)) SETTINGS use_client_time_zone = 0;
")
if [[ "$actual_output" != "$expected_output" ]]; then
    printf 'Expected:\n%s\nActual:\n%s\n' "$expected_output" "$actual_output"
    exit 1
fi
echo 'query-local server timezone matches default'

$TZC --use_client_time_zone=1 -mn -q "
CREATE TEMPORARY TABLE client_timezone_switch (dt DateTime, dt64 DateTime64(3), kind String) ENGINE = Memory;
INSERT INTO client_timezone_switch SETTINGS use_client_time_zone = 0, async_insert = 0
    VALUES ('2017-07-14 05:40:00', '2017-07-14 05:40:00.125', 'server');
INSERT INTO client_timezone_switch SETTINGS async_insert = 0
    VALUES ('2017-07-14 05:40:00', '2017-07-14 05:40:00.125', 'client');
SELECT 'query-local sync uses server tz',
    toUnixTimestamp(dt) = toUnixTimestamp(toDateTime('2017-07-14 05:40:00', serverTimeZone())),
    toUnixTimestamp64Milli(dt64) = toUnixTimestamp64Milli(toDateTime64('2017-07-14 05:40:00.125', 3, serverTimeZone()))
FROM client_timezone_switch WHERE kind = 'server';
SELECT 'query-local sync restores client tz',
    toUnixTimestamp(dt) = 1500036000,
    toUnixTimestamp64Milli(dt64) = 1500036000125
FROM client_timezone_switch WHERE kind = 'client';
DROP TABLE client_timezone_switch;
"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_DATABASE}.dt"
