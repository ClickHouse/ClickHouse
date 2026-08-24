#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

stderr="$(mktemp "$CURDIR/clickhouse.stderr.XXXXXX.log")"
trap 'rm -f "$stderr"' EXIT

function process_log_safe()
{
    grep "^\\[" "$@" | sed -e 's/.*> //' -e 's/, reason.*//' -e 's/ DB::NetException//' -e 's/ Log: //'
}
function execute_query()
{
    local hosts=$1 && shift
    local opts=(
        "--connections_with_failover_max_tries=1"
        "--skip_unavailable_shards=1"
    )

    echo "$hosts"
    # NOTE: we cannot use process substition here for simplicity because they are async, i.e.:
    #
    #   clickhouse-client 2> >(wc -l)
    #
    # May dump output of "wc -l" after some other programs.
    $CLICKHOUSE_CLIENT "${opts[@]}" --query "select * from remote('$hosts', system.one) settings use_hedged_requests=0" 2>"$stderr"
    process_log_safe "$stderr"
}
execute_query 255.255.255.255
execute_query 127.2,255.255.255.255
# This will print two errors because there will be two attempts for 255.255.255.255:
# - first for obtaining structure of the table
# - second for the query
execute_query 255.255.255.255,127.2
