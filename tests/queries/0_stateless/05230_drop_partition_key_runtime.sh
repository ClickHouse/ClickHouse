#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

suffix=$(printf '%s' "$CLICKHOUSE_TEST_UNIQUE_NAME" | tr -cd '[:alnum:]_')
runtime_table="t_drop_partition_key_runtime_${suffix}"
rbac_table="t_drop_partition_key_rbac_${suffix}"
user="drop_partition_key_user_${suffix}"

query_as()
{
    local query_user="$1"
    local query="$2"
    "$CLICKHOUSE_CLIENT_BINARY" --database="$CLICKHOUSE_DATABASE" --user="$query_user" --query="$query"
}

query_err_as()
{
    local query_user="$1"
    local query="$2"
    "$CLICKHOUSE_CLIENT_BINARY" --database="$CLICKHOUSE_DATABASE" --user="$query_user" --query="$query" 2>&1 || true
}

cleanup()
{
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${runtime_table}"
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${rbac_table}"
    $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${user}"
}

trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT --query "CREATE TABLE ${runtime_table} (d Date, id UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY id"
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES ${runtime_table}"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${runtime_table} VALUES ('2024-01-01', 1)"
$CLICKHOUSE_CLIENT --query "INSERT INTO ${runtime_table} VALUES ('2024-01-02', 2)"
$CLICKHOUSE_CLIENT --query "ALTER TABLE ${runtime_table} DROP PARTITION KEY"

part=$($CLICKHOUSE_CLIENT --query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = '${runtime_table}' AND active ORDER BY name LIMIT 1")
$CLICKHOUSE_CLIENT --query "ALTER TABLE ${runtime_table} DETACH PART '${part}'"
$CLICKHOUSE_CLIENT --query "ALTER TABLE ${runtime_table} ATTACH PART '${part}'"
test "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM ${runtime_table}")" = "2"
echo "attach after drop: OK"

$CLICKHOUSE_CLIENT --query "CREATE TABLE ${rbac_table} (d Date, id UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY id"
$CLICKHOUSE_CLIENT --query "CREATE USER ${user}"

grep -q "Not enough privileges" <<<"$(query_err_as "$user" "ALTER TABLE ${rbac_table} DROP PARTITION KEY")"
echo "rbac deny: OK"

$CLICKHOUSE_CLIENT --query "GRANT ALTER PARTITION BY ON ${rbac_table} TO ${user}"
query_as "$user" "ALTER TABLE ${rbac_table} DROP PARTITION KEY"
echo "rbac grant: OK"
