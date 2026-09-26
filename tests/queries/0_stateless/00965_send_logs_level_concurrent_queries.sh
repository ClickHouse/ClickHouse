#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for _ in {1..10}; do
    # A query at trace level must deliver verbose (<Debug>/<Trace>) logs to the client, while the same
    # query at information level must not -- that threshold is what this test checks. Match the marker
    # anywhere on the line: the log prefix omits host_name/query_id when empty, so their columns are not
    # fixed and a positional awk '{print $8}' is fragile. Accept <Debug> as well as <Trace>: a trivial
    # SELECT is not guaranteed to emit a <Trace>-priority line (the LOG_TRACE calls on the query path are
    # all conditional), but `executeQuery` always logs the incoming query at <Debug>, so requiring
    # <Debug>|<Trace> is deterministic and stays symmetric with the information-level negative check.
    ${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="trace" --query="SELECT * from numbers(1000000);" 2>&1 | grep -q '<Debug>\|<Trace>' && echo "OK" || echo "Fail" &
    ${CLICKHOUSE_CLIENT_BINARY} --send_logs_level="information" --query="SELECT * from numbers(1000000);" 2>&1 | grep '<Debug>\|<Trace>' &
done

wait
