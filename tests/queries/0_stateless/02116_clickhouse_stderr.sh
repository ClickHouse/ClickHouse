#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

(
    echo 'Ensure that not existing file still write an error to stderr'

    test_dir=$(mktemp -d -t clickhouse.XXXXXX)
    mkdir -p "$test_dir"
    cd "$test_dir" || exit 1
    $CLICKHOUSE_SERVER_BINARY -- --logger.stderr=/no/such/file |& grep -o 'File /no/such/file (logger.stderr) is not writable'
    rm -fr "${test_dir:?}"
)

(
    echo 'Ensure that the file will be created'

    test_dir=$(mktemp -d -t clickhouse.XXXXXX)
    mkdir -p "$test_dir"
    cd "$test_dir" || exit 1

    stderr=$(mktemp -t clickhouse.XXXXXX)
    # It will fail to start because 192.0.2.1 (a reserved documentation address, RFC 5737) cannot be
    # bound. This is a deterministic post-logger-init failure on every OS, unlike relying on a port
    # collision, which does not fire on macOS where the running server binds only IPv6 (::).
    $CLICKHOUSE_SERVER_BINARY -- --listen_host 192.0.2.1 --tcp_port "$CLICKHOUSE_PORT_TCP" --logger.stderr="$stderr" 2>/dev/null
    # -s -- check that stderr was created and is not empty
    test -s "$stderr" || exit 2
    rm "$stderr"

    rm -fr "${test_dir:?}"
)
