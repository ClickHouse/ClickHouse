#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Test that clickhouse-local supports SYSTEM START/STOP LISTEN.
# Default ports (tcp_port=9000, http_port=8123) are preconfigured,
# so SYSTEM START LISTEN works without specifying ports explicitly.

$CLICKHOUSE_LOCAL --query "
    SYSTEM START LISTEN TCP;
    SELECT 'tcp_started';
    SYSTEM START LISTEN HTTP;
    SELECT 'http_started';
    SYSTEM STOP LISTEN TCP;
    SELECT 'tcp_stopped';
    SYSTEM STOP LISTEN HTTP;
    SELECT 'http_stopped';
"
