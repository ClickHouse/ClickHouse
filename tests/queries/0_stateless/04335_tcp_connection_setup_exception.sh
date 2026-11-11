#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# - no-fasttest: fail points are not available, the secure port is not enabled
# - no-parallel: the fail point affects every new TCP connection to the server
# When the connection setup fails (for example, the allocation of the connection buffers
# fails because the server memory limit is reached), the client must receive the exception
# instead of `Connection reset by peer`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The fail point is enabled and disabled over HTTP because TCP connections fail while it is active.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SYSTEM ENABLE FAILPOINT tcp_handler_fail_connection_setup"

${CLICKHOUSE_CLIENT} -q "SELECT 1" |& grep -o -m1 -e MEMORY_LIMIT_EXCEEDED -e NETWORK_ERROR -e "Connection reset by peer"

# The drain of the socket before closing works differently for secure connections,
# so they are checked separately.
${CLICKHOUSE_CLIENT_SECURE} -q "SELECT 1" |& grep -o -m1 -e MEMORY_LIMIT_EXCEEDED -e NETWORK_ERROR -e "Connection reset by peer"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SYSTEM DISABLE FAILPOINT tcp_handler_fail_connection_setup"

${CLICKHOUSE_CLIENT} -q "SELECT 1"
${CLICKHOUSE_CLIENT_SECURE} -q "SELECT 1"
