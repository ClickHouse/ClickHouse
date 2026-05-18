#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `clickhouse-local` only manages TCP and HTTP listeners. Grouped types
# (`QUERIES ALL` / `QUERIES DEFAULT`) are accepted when they cover at least
# one of TCP or HTTP, but must be rejected when their `EXCEPT` list strips
# out both — otherwise the operation would silently no-op.

# Both supported protocols excluded → reject.
$CLICKHOUSE_LOCAL --query "SYSTEM START LISTEN QUERIES ALL EXCEPT TCP, HTTP; -- { serverError UNSUPPORTED_METHOD }"
$CLICKHOUSE_LOCAL --query "SYSTEM STOP LISTEN QUERIES ALL EXCEPT TCP, HTTP; -- { serverError UNSUPPORTED_METHOD }"
$CLICKHOUSE_LOCAL --query "SYSTEM START LISTEN QUERIES DEFAULT EXCEPT TCP, HTTP; -- { serverError UNSUPPORTED_METHOD }"
$CLICKHOUSE_LOCAL --query "SYSTEM STOP LISTEN QUERIES DEFAULT EXCEPT TCP, HTTP; -- { serverError UNSUPPORTED_METHOD }"

# Only one supported protocol excluded → request still touches the other,
# so it must succeed. `STOP` on a fresh process is a no-op.
$CLICKHOUSE_LOCAL --query "SYSTEM STOP LISTEN QUERIES ALL EXCEPT TCP;"
$CLICKHOUSE_LOCAL --query "SYSTEM STOP LISTEN QUERIES ALL EXCEPT HTTP;"
$CLICKHOUSE_LOCAL --query "SYSTEM STOP LISTEN QUERIES DEFAULT EXCEPT TCP;"
$CLICKHOUSE_LOCAL --query "SYSTEM STOP LISTEN QUERIES DEFAULT EXCEPT HTTP;"

# `START` with only one of TCP/HTTP excluded should actually start the other one.
$CLICKHOUSE_LOCAL --listen_host 127.0.0.1 --tcp_port 0 --http_port 0 --query "
    SYSTEM START LISTEN QUERIES ALL EXCEPT HTTP;
    SELECT 'tcp_only_started';
"

$CLICKHOUSE_LOCAL --listen_host 127.0.0.1 --tcp_port 0 --http_port 0 --query "
    SYSTEM START LISTEN QUERIES ALL EXCEPT TCP;
    SELECT 'http_only_started';
"
