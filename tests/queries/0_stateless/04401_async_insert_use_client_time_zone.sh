#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings
# ^ no-random-settings: the runner must not inject a randomized `session_timezone`; an explicit
#   `session_timezone` (even empty) is an intentional user override and disables the client-time-zone
#   propagation this test exercises.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# With --use_client_time_zone=1, a DateTime string literal must be interpreted in the client time
# zone regardless of whether the INSERT is synchronous or asynchronous. The async path parses the
# VALUES block on the server, so the client has to propagate its local time zone as session_timezone.
# America/Hermosillo is a fixed UTC-7 zone (no DST), so the expected value is stable.

${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.dt (a DateTime, kind String) ENGINE = Memory"

env TZ=America/Hermosillo ${CLICKHOUSE_CLIENT} --use_client_time_zone=1 -q \
  "INSERT INTO ${CLICKHOUSE_DATABASE}.dt SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES ('2017-07-14 05:40:00', 'async')"

env TZ=America/Hermosillo ${CLICKHOUSE_CLIENT} --use_client_time_zone=1 -q \
  "INSERT INTO ${CLICKHOUSE_DATABASE}.dt SETTINGS async_insert = 0 VALUES ('2017-07-14 05:40:00', 'sync')"

# Both rows must store the same instant: 2017-07-14 05:40:00 in America/Hermosillo = 1500036000.
${CLICKHOUSE_CLIENT} -q "SELECT kind, toUnixTimestamp(a) FROM ${CLICKHOUSE_DATABASE}.dt ORDER BY kind"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${CLICKHOUSE_DATABASE}.dt"
