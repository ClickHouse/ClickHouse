#!/bin/zsh
# Works with bash and zsh

# It should be used with `runner --debug`
# Tip: place a `breakpoint()` somewhere in your integration test python code before run.

_runner-select-node() {
    RUNNER_ID1=$(docker ps | grep praktika | awk '{print $1; exit}')
    RUNNER_ID2_DEFAULT=$(docker exec $RUNNER_ID1 docker ps | grep node | awk '{print $1; exit}')
    docker exec $RUNNER_ID1 docker ps
    RUNNER_ID2=
    echo -n "Enter ClickHouse Node CONTAINER ID or NAME (default: $RUNNER_ID2_DEFAULT): "
    read RUNNER_ID2
    RUNNER_ID2=${RUNNER_ID2:-$RUNNER_ID2_DEFAULT}
}

# Runs a clickhouse client for a node inside an integration test.
runner-client() {
    _runner-select-node
    docker exec -it $RUNNER_ID1 docker exec -it $RUNNER_ID2 clickhouse client
}

# Opens shell on a node inside an integration test.
runner-bash() {
    _runner-select-node
    docker exec -it $RUNNER_ID1 docker exec -it $RUNNER_ID2 bash
}

# Attaches nnd debugger to a clickhouse server on a node inside an integration test.
# You have to put statically-linked nnd binary into ClickHouse root folder
runner-nnd() {
    _runner-select-node
    RUNNER_PID=$(docker exec $RUNNER_ID1 docker exec $RUNNER_ID2 sh -c 'ps aux | grep -v grep | grep -v bash | grep -m 1 "clickhouse server" | awk "{print \$2}"')
    docker exec -it $RUNNER_ID1 docker exec -it $RUNNER_ID2 /debug/nnd -p $RUNNER_PID -d /debug
}

echo "USAGE:"
echo "   runner-client - Run clickhouse client inside an integration test"
echo "   runner-bash   - Open shell on a node inside an integration test"
echo "   runner-nnd    - Attach nnd debugger to a clickhouse server on a node inside an integration test"
echo ""
