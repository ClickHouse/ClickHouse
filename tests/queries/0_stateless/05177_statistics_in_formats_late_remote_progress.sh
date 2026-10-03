#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A remote server reports the rows it has read in the trailing packets of its connection, and the
# initiator drains these packets only after `LIMIT` has been satisfied. The output format used to
# write its statistics as soon as its inputs were exhausted, so `rows_read` missed the progress of
# a remote server that was still pending at that moment.
#
# `LIMIT` is satisfied by the local part of the `UNION ALL`. The remote part returns no rows at all
# (the `sleep` in its `WHERE` filters everything out), so the 6 rows it reads are reported only when
# its connection is drained, after the data path of the query has already finished. `interactive_delay`
# keeps the remote server from sending its progress before the end of its query, an asynchronous
# socket keeps the initiator from blocking on the connection until then, and a single thread fixes
# the order of the draining and of the finalization of the output format.
#
# `rows_before_limit_at_least` is checked together with `rows_read`: the final value of its counter
# is picked up from the same place, when the whole pipeline has finished, both for the output format
# of the HTTP interface and for the `ProfileInfo` of the native protocol.

QUERY="SELECT number FROM
(
    SELECT number FROM numbers(5) WHERE sleep(0.1) = 0
    UNION ALL
    SELECT number FROM remote('127.0.0.3', view(SELECT number FROM numbers(6) WHERE sleep(0.5) = 1))
)
LIMIT 5
SETTINGS prefer_localhost_replica = 0, async_socket_for_remote = 1, use_concurrency_control = 0, max_threads = 1, interactive_delay = 1000000"

for format in JSON JSONCompact XML
do
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "$QUERY FORMAT $format" | grep -E 'rows_read|rows_before_limit_at_least'
    $CLICKHOUSE_CLIENT --query "$QUERY FORMAT $format" | grep -E 'rows_read|rows_before_limit_at_least'
done
