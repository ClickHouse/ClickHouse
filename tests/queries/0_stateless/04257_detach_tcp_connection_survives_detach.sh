#!/usr/bin/env bash
# Regression test for `TCPHandler::runImpl`: after a detached query, the connection loop
# must iterate (`continue`) rather than exit (`return`), so the same TCP connection still
# serves subsequent queries on the wire and per-connection session state survives.
#
# Two queries are sent in a single `--multiquery` invocation, which routes them over the
# same connection. The first runs in detach mode and the server replies with the detached
# `query_id`. The second disables detach inline so it runs synchronously; its result must
# come back on the same connection. With the old `return;`, the second query would hit a
# closed socket and never produce `connection_alive`.

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# Set before sourcing shell_config so CLICKHOUSE_CLIENT etc. use the build binary
export CLICKHOUSE_BINARY="${CLICKHOUSE_BINARY:-${CURDIR}/../../../build/programs/clickhouse}"
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Unique query_id per run to avoid QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING in parallel runs.
QID="04257_$$_${RANDOM}_detached"

OUT=$($CLICKHOUSE_CLIENT \
    --allow_experimental_detach_queries 1 \
    --async_insert 0 \
    --query_id "${QID}" \
    --multiquery -q "
        SELECT 1 FORMAT TabSeparated;
        SELECT 'connection_alive' SETTINGS allow_experimental_detach_queries=0 FORMAT TabSeparated;
    " 2>&1) || true

# The sync follow-up reaching us is the whole point of the `continue;` change.
if echo "$OUT" | grep -q "connection_alive"; then
    echo "Follow-up sync query returned on same connection: yes"
else
    echo "Follow-up sync query returned on same connection: no"
    echo "FAIL: TCP connection did not survive the detached query."
    echo "Captured output:"
    echo "$OUT"
    exit 1
fi

# Sanity: a non-empty query_id from the detached first query appeared too.
LINES_BEFORE=$(echo "$OUT" | grep -vE '^\s*$' | grep -vF 'connection_alive' | wc -l)
if [ "$LINES_BEFORE" -ge 1 ]; then
    echo "Detached query returned a non-empty query_id: yes"
else
    echo "Detached query returned a non-empty query_id: no"
    echo "FAIL: Expected query_id from the detached query before the sync follow-up."
    echo "Captured output:"
    echo "$OUT"
    exit 1
fi

echo "OK"
