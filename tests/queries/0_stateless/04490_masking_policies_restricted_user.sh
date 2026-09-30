#!/usr/bin/env bash

# A user granted only `SHOW MASKING POLICIES` must be able to introspect masking policies, both via
# `SHOW MASKING POLICIES` (which is rewritten to `SELECT name FROM system.masking_policies`) and by
# selecting from `system.masking_policies` directly, without an explicit `SELECT` grant on the system
# table. This requires the implicit `SELECT` grant on `system.masking_policies` derived from the
# `SHOW MASKING POLICIES` privilege when `select_from_system_db_requires_grant` is enabled (it is in CI).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER="user_04490_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${USER}"
$CLICKHOUSE_CLIENT --query "CREATE USER ${USER} IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT --query "GRANT SHOW MASKING POLICIES ON *.* TO ${USER}"

echo "=== SHOW MASKING POLICIES (empty, no error) ==="
$CLICKHOUSE_CLIENT --user="${USER}" --query "SHOW MASKING POLICIES"

echo "=== SELECT count() FROM system.masking_policies (implicit SELECT grant) ==="
$CLICKHOUSE_CLIENT --user="${USER}" --query "SELECT count() FROM system.masking_policies"

$CLICKHOUSE_CLIENT --query "DROP USER ${USER}"
