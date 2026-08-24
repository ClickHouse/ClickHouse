#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

export SESSION="03403_session_${CLICKHOUSE_DATABASE}"
export SESSION_ID="${SESSION}_$RANDOM.$RANDOM"
export SETTINGS="session_id=$SESSION_ID&session_timeout=2"

# The general idea behind the test is to check that if the session was closed,
# we won't try to close a newly created session with the same name (and cause some inconsistent state by it)
$CLICKHOUSE_CURL -sS -d 'select 1' "$CLICKHOUSE_URL&$SETTINGS&close_session=1"
# Sleep some more time to give more chances for a cleanThread in Session.cpp to schedule a close for this session
$CLICKHOUSE_CURL -sS -d 'select sleep(5) settings function_sleep_max_microseconds_per_block = 5000000' "$CLICKHOUSE_URL&$SETTINGS"
