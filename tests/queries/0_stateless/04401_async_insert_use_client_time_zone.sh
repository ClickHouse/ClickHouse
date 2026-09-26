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

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_DATABASE}.dt"
