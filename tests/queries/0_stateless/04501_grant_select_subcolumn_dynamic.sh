#!/usr/bin/env bash

# Regression test: granting SELECT on a JSON (or Dynamic) parent column must implicitly grant
# access to its dynamic subcolumns (e.g. `json.a.b`). Dynamic paths are not part of the type's
# static subcolumn list, so the access-check normalization must fall back to the parent column
# for them, otherwise the check runs against `json.a.b` and `GRANT SELECT(json)` fails.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER="test_user_04501_${CLICKHOUSE_DATABASE}"
DB="${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER}"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${DB}.t_json_sub"

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.t_json_sub (id UInt32, json JSON) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${DB}.t_json_sub VALUES (1, '{\"a\": {\"b\": 42}}')"

$CLICKHOUSE_CLIENT -q "CREATE USER ${USER}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(json) ON ${DB}.t_json_sub TO ${USER}"

# Reading a dynamic subcolumn of the granted JSON column must be allowed. Assert on access
# (success vs ACCESS_DENIED) rather than the value, whose Dynamic formatting is not the point.
echo "=== select json.a.b with SELECT(json) ==="
if $CLICKHOUSE_CLIENT --user "${USER}" -q "SELECT json.a.b FROM ${DB}.t_json_sub FORMAT Null" 2>/dev/null; then
    echo "OK"
else
    echo "UNEXPECTED_DENIED"
fi

# A column that was not granted must still be denied.
echo "=== select id (denied) ==="
$CLICKHOUSE_CLIENT --user "${USER}" -q "SELECT id FROM ${DB}.t_json_sub" 2>&1 | grep -o 'ACCESS_DENIED' | head -1

# After revoking the parent grant, the dynamic subcolumn must be denied too.
$CLICKHOUSE_CLIENT -q "REVOKE SELECT(json) ON ${DB}.t_json_sub FROM ${USER}"
echo "=== select json.a.b after revoke (denied) ==="
$CLICKHOUSE_CLIENT --user "${USER}" -q "SELECT json.a.b FROM ${DB}.t_json_sub" 2>&1 | grep -o 'ACCESS_DENIED' | head -1

$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.t_json_sub"
$CLICKHOUSE_CLIENT -q "DROP USER ${USER}"
