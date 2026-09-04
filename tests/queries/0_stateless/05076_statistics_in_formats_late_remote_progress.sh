#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A remote server sends its reading progress after the data, and the initiator drains the remaining packets
# of the connection only after `LIMIT` has been satisfied. The output format used to write its statistics
# as soon as the last row had arrived, so `rows_read` could miss the progress of a remote server whose
# trailing packets were late.
#
# The first remote query finishes at once. The second one sends its data at once too, but finishes only after
# `sleep`, so its progress arrives when the initiator is already done with the data. `interactive_delay` keeps
# the remote server from noticing the cancellation before it has sent its progress, and two threads let the
# output format be finalized concurrently with the draining of the connection.

QUERY="SELECT number FROM
(
    SELECT number FROM remote('127.0.0.2', numbers(5))
    UNION ALL
    SELECT number FROM remote('127.0.0.3', view(SELECT number FROM numbers(5) UNION ALL SELECT number FROM numbers(1) WHERE sleep(0.3) = 1))
)
LIMIT 5
SETTINGS prefer_localhost_replica = 0, use_concurrency_control = 0, max_threads = 2, interactive_delay = 1000000"

for format in JSON JSONCompact XML
do
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "$QUERY FORMAT $format" | grep 'rows_read'
    $CLICKHOUSE_CLIENT --query "$QUERY FORMAT $format" | grep 'rows_read'
done
