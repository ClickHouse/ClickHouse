#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="$CUR_DIR/$(basename "${BASH_SOURCE[0]}" .sh)"

server_pids=()
paths=()

function cleanup()
{
    local pid
    for pid in "${server_pids[@]}"; do
        kill -9 "$pid"
    done

    echo "Test failed." >&2
    tail -n1000 "$BASE".clickhouse-server*.log "$BASE".clickhouse-benchmark*.log >&2
    rm -f "$BASE".clickhouse-server*.log "$BASE".clickhouse-benchmark*.log

    local path
    for path in "${paths[@]}"; do
        rm -fr "$path"
    done

    exit 1
}

function start_server()
{
    local log=$1 && shift

    local server_opts=(
        "--config-file=$BASE.config.xml"
        "--"
        # we will discover the real port later.
        "--tcp_port=0"
        "--shutdown_wait_unfinished=0"
        "--listen_host=127.1"
    )
    CLICKHOUSE_WATCHDOG_ENABLE=0 $CLICKHOUSE_SERVER_BINARY "${server_opts[@]}" "$@" >& "$log" &
    local pid=$!

    echo "$pid"
}

function get_server_port()
{
    local pid=$1 && shift
    local port='' i=0 retries=300
    # wait until server will start to listen (max 30 seconds)
    while [[ -z $port ]] && [[ $i -lt $retries ]]; do
        port="$(lsof -n -a -P -i tcp -s tcp:LISTEN -p "$pid" 2>/dev/null | awk -F'[ :]' '/LISTEN/ { print $(NF-1) }')"
        ((++i))
        sleep 0.1
    done
    if [[ -z $port ]]; then
        echo "Cannot wait for LISTEN socket" >&2
        exit 1
    fi
    echo "$port"
}

function wait_server_port()
{
    local port=$1 && shift
    # wait for the server to start accepting tcp connections (max 30 seconds)
    local i=0 retries=300
    while ! $CLICKHOUSE_CLIENT_BINARY --host 127.1 --port "$port" --format Null -q 'select 1' 2>/dev/null && [[ $i -lt $retries ]]; do
        sleep 0.1
    done
    if ! $CLICKHOUSE_CLIENT_BINARY --host 127.1 --port "$port" --format Null -q 'select 1'; then
        echo "Cannot wait until server will start accepting connections on <tcp_port>" >&2
        exit 1
    fi
}

function execute_query()
{
    local port=$1 && shift
    $CLICKHOUSE_CLIENT_BINARY --host 127.1 --port "$port" "$@"
}

function make_server()
{
    local log=$1 && shift

    local pid
    pid="$(start_server "$log" "$@")"

    local port
    port="$(get_server_port "$pid")"
    wait_server_port "$port"

    echo "$pid" "$port"
}

function terminate_servers()
{
    local pid
    for pid in "${server_pids[@]}"; do
        kill -9 "$pid"
        # NOTE: we cannot wait the server pid since it was created in a subshell
    done

    rm -f "$BASE".clickhouse-server*.log "$BASE".clickhouse-benchmark*.log

    local path
    for path in "${paths[@]}"; do
        rm -fr "$path"
    done
}

function test_clickhouse_benchmark_multi_hosts()
{
    local benchmark_opts=(
        --iterations 10000
        --host 127.1 --port "$port1"
        --host 127.1 --port "$port2"
        --query 'select 1'
        --concurrency 10
    )
    clickhouse-benchmark "${benchmark_opts[@]}" >& "$(mktemp "$BASE.clickhouse-benchmark.XXXXXX.log")"

    local queries1 queries2
    queries1="$(execute_query "$port1" --query "select value from system.events where event = 'Query'")"
    queries2="$(execute_query "$port2" --query "select value from system.events where event = 'Query'")"

    if [[ $queries1 -lt 4000 ]] || [[ $queries1 -gt 6000 ]]; then
        echo "server1 (port=$port1) handled $queries1 queries" >&2
    fi
    if [[ $queries2 -lt 4000 ]] || [[ $queries2 -gt 6000 ]]; then
        echo "server1 (port=$port2) handled $queries2 queries" >&2
    fi
}
function test_clickhouse_benchmark_multi_hosts_roundrobin()
{
    local benchmark_opts=(
        --iterations 10000
        --host 127.1 --port "$port1"
        --host 127.1 --port "$port2"
        --query 'select 1'
        --concurrency 10
        --roundrobin
    )
    clickhouse-benchmark "${benchmark_opts[@]}" >& "$(mktemp "$BASE.clickhouse-benchmark.XXXXXX.log")"

    local queries1 queries2
    queries1="$(execute_query "$port1" --query "select value from system.events where event = 'Query'")"
    queries2="$(execute_query "$port2" --query "select value from system.events where event = 'Query'")"

    # NOTE: it should take into account test_clickhouse_benchmark_multi_hosts queries too.
    # that's why it is [9000, 11000] instead of [4000, 6000]
    if [[ $queries1 -lt 9000 ]] || [[ $queries1 -gt 11000 ]]; then
        echo "server1 (port=$port1) handled $queries1 queries (with --roundrobin)" >&2
    fi
    if [[ $queries2 -lt 9000 ]] || [[ $queries2 -gt 11000 ]]; then
        echo "server1 (port=$port2) handled $queries2 queries (with --roundrobin)" >&2
    fi
}

function main()
{
    trap cleanup EXIT

    local path port1 port2

    path="$(mktemp -d "$BASE.server1.XXXXXX")"
    paths+=( "$path" )
    read -r pid1 port1 <<<"$(make_server "$(mktemp "$BASE.clickhouse-server-XXXXXX.log")" --path "$path")"
    server_pids+=( "$pid1" )

    path="$(mktemp -d "$BASE.server2.XXXXXX")"
    paths+=( "$path" )
    read -r pid2 port2 <<<"$(make_server "$(mktemp "$BASE.clickhouse-server-XXXXXX.log")" --path "$path")"
    server_pids+=( "$pid2" )

    test_clickhouse_benchmark_multi_hosts
    test_clickhouse_benchmark_multi_hosts_roundrobin

    terminate_servers
    trap '' EXIT
}
main "$@"
