#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `trace` column of `system.stack_trace` contains absolute (runtime) virtual addresses, which
# disclose the load bases of the process, so reading it requires the `INTROSPECTION` privilege.
# The other columns stay available with a plain `SELECT` grant.

user="user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS $user"
$CLICKHOUSE_CLIENT --query "CREATE USER $user NOT IDENTIFIED"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON system.stack_trace TO $user"

echo 'without INTROSPECTION:'
$CLICKHOUSE_CLIENT --user "$user" --query "SELECT max(thread_id) > 0 FROM system.stack_trace"
$CLICKHOUSE_CLIENT --user "$user" --query "SELECT trace FROM system.stack_trace LIMIT 1 FORMAT Null" 2>&1 | grep -o 'ACCESS_DENIED' | head -n 1

$CLICKHOUSE_CLIENT --query "GRANT INTROSPECTION ON *.* TO $user"

echo 'with INTROSPECTION:'
$CLICKHOUSE_CLIENT --user "$user" --query "SELECT length(trace) >= 0 FROM system.stack_trace LIMIT 1"

$CLICKHOUSE_CLIENT --query "DROP USER $user"
