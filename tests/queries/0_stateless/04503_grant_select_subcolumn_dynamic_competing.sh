#!/usr/bin/env bash

# Regression test for access checks on dynamic subcolumns when a shorter dynamic column and a
# longer dotted real column coexist (e.g. `json JSON` and `json.a JSON`, where `json.a` is a real
# column whose name contains a dot).
#
# Identifier resolution scans dotted prefixes left to right and stops at the first prefix whose type
# can provide the subcolumn, so `SELECT json.a.b` reads from `json` (JSON can provide the path
# `a.b`), never from the `json.a` column. The access check must authorize the same column: it must
# require `SELECT(json)`, NOT `SELECT(json.a)`. Authorizing the longest existing dotted prefix
# (`json.a`) instead would let a grant on `json.a` alone read the whole `json` column.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER="test_user_04503_${CLICKHOUSE_DATABASE}"
DB="${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER}"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${DB}.t_json_competing"

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.t_json_competing (id UInt32, json JSON, \`json.a\` JSON) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${DB}.t_json_competing (id, json, \`json.a\`) VALUES (1, '{\"a\": {\"b\": 42}}', '{\"b\": 1}')"

$CLICKHOUSE_CLIENT -q "CREATE USER ${USER}"

# A grant on the dotted real column `json.a` must NOT authorize `json.a.b`, which resolves through
# `json`. Assert on access (ACCESS_DENIED) rather than the value.
$CLICKHOUSE_CLIENT -q "GRANT SELECT(\`json.a\`) ON ${DB}.t_json_competing TO ${USER}"

echo "=== json.a.b with SELECT(\`json.a\`) only (denied) ==="
$CLICKHOUSE_CLIENT --user "${USER}" -q "SELECT json.a.b FROM ${DB}.t_json_competing FORMAT Null" 2>&1 | grep -o 'ACCESS_DENIED' | head -1

echo "=== \`json.a\` column with SELECT(\`json.a\`) (allowed) ==="
if $CLICKHOUSE_CLIENT --user "${USER}" -q "SELECT \`json.a\` FROM ${DB}.t_json_competing FORMAT Null" 2>/dev/null; then
    echo "OK"
else
    echo "UNEXPECTED_DENIED"
fi

# Swap the grant: `SELECT(json)` must cover the dynamic subcolumn `json.a.b` ...
$CLICKHOUSE_CLIENT -q "REVOKE SELECT(\`json.a\`) ON ${DB}.t_json_competing FROM ${USER}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(json) ON ${DB}.t_json_competing TO ${USER}"

echo "=== json.a.b with SELECT(json) (allowed) ==="
if $CLICKHOUSE_CLIENT --user "${USER}" -q "SELECT json.a.b FROM ${DB}.t_json_competing FORMAT Null" 2>/dev/null; then
    echo "OK"
else
    echo "UNEXPECTED_DENIED"
fi

# ... but must NOT cover the separate `json.a` real column.
echo "=== \`json.a\` column with SELECT(json) only (denied) ==="
$CLICKHOUSE_CLIENT --user "${USER}" -q "SELECT \`json.a\` FROM ${DB}.t_json_competing FORMAT Null" 2>&1 | grep -o 'ACCESS_DENIED' | head -1

$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.t_json_competing"
$CLICKHOUSE_CLIENT -q "DROP USER ${USER}"
