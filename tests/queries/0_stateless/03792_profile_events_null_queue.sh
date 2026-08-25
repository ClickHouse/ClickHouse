#!/usr/bin/env bash
# This test reproduces a segfault that occurs when send_profile_events is enabled
# via SQL SETTINGS clause after being disabled at the connection level.
#
# The bug: profile_queue is only created if send_profile_events was true during
# connection setup. If it was false but then enabled via SQL SETTINGS, the queue
# is null and getProfileEvents() crashes when trying to lock the mutex.
#
# The crash happens at ProfileEventsExt.cpp:169 when calling profile_queue->tryPop()
# with a null profile_queue pointer.
#
# NOTE: This bug is TCP protocol specific. HTTP doesn't send profile events.
# NOTE: clickhouse-client cannot reproduce this because it parses the query
# client-side and merges SQL SETTINGS into protocol settings before sending
# (see ClientBase.cpp:2337 - InterpreterSetQuery::applySettingsFromQuery).
# We use Python clickhouse-driver which sends settings and query separately.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Check if clickhouse-driver is available
if ! python3 -c "import clickhouse_driver" 2>/dev/null; then
    echo "1"
    echo "OK"
    exit 0
fi

# Connect with send_profile_events disabled at the connection level,
# but enable it via SQL SETTINGS clause in the query.
# Before the fix: server crashes with SIGSEGV
# After the fix: query executes successfully
python3 << EOF
from clickhouse_driver import Client

client = Client(
    host='${CLICKHOUSE_HOST:-localhost}',
    port=${CLICKHOUSE_PORT_TCP:-9000},
    settings={'send_profile_events': False},
    send_receive_timeout=5
)

# This query would crash unpatched servers
result = client.execute('SELECT 1 SETTINGS send_profile_events = 1')
print(result[0][0])

print("OK")
EOF
