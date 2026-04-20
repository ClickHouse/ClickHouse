#!/usr/bin/env bash
# Tags: no-parallel

# Tests that granting SELECT on a compound column implicitly grants access
# to all of its subcolumns (Tuple elements, Map keys/values, Array size0,
# Nullable null, QBit slices, etc.).
# See the bug report: SELECT on `t` is granted but `t.a` returned ACCESS_DENIED.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

USER="test_user_04104_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER}"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_sub_access"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE t_sub_access
(
    id UInt32,
    t Tuple(a UInt32, b String),
    m Map(String, UInt32),
    arr Array(UInt32),
    n Nullable(UInt32),
    vec QBit(BFloat16, 3)
) ENGINE = Memory
"

$CLICKHOUSE_CLIENT -q "INSERT INTO t_sub_access VALUES (1, (10, 'hello'), {'key1': 100}, [1, 2, 3], 42, [1.0, 2.0, 3.0])"

$CLICKHOUSE_CLIENT -q "CREATE USER ${USER}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(t, m, arr, n, vec) ON ${CLICKHOUSE_DATABASE}.t_sub_access TO ${USER}"

run_as_user()
{
    local label="$1"
    local query="$2"
    echo "=== ${label} ==="
    $CLICKHOUSE_CLIENT --user "${USER}" -q "${query}" 2>&1 | sed 's/.*DB::Exception:.*\(ACCESS_DENIED\).*/\1/'
}

# Parent-column queries (expected OK even before the fix).
run_as_user "select t"      "SELECT t FROM ${CLICKHOUSE_DATABASE}.t_sub_access"
run_as_user "select m"      "SELECT m FROM ${CLICKHOUSE_DATABASE}.t_sub_access"
run_as_user "select arr"    "SELECT arr FROM ${CLICKHOUSE_DATABASE}.t_sub_access"
run_as_user "select n"      "SELECT n FROM ${CLICKHOUSE_DATABASE}.t_sub_access"
run_as_user "select vec"    "SELECT vec FROM ${CLICKHOUSE_DATABASE}.t_sub_access"

# Subcolumn queries (these are the ones previously failing with ACCESS_DENIED).
run_as_user "select t.a"        "SELECT t.a FROM ${CLICKHOUSE_DATABASE}.t_sub_access"
run_as_user "select t.b"        "SELECT t.b FROM ${CLICKHOUSE_DATABASE}.t_sub_access"
run_as_user "select m.keys"     "SELECT m.keys FROM ${CLICKHOUSE_DATABASE}.t_sub_access"
run_as_user "select m.values"   "SELECT m.values FROM ${CLICKHOUSE_DATABASE}.t_sub_access"
run_as_user "select arr.size0"  "SELECT arr.size0 FROM ${CLICKHOUSE_DATABASE}.t_sub_access"
run_as_user "select n.null"     "SELECT n.null FROM ${CLICKHOUSE_DATABASE}.t_sub_access"
run_as_user "select vec.1"      "SELECT vec.1 FROM ${CLICKHOUSE_DATABASE}.t_sub_access"

# Negative check: a column that was NOT granted must still be denied.
run_as_user "select id (denied)" "SELECT id FROM ${CLICKHOUSE_DATABASE}.t_sub_access"

# Negative check: subcolumn access must be denied if the parent column is not granted.
$CLICKHOUSE_CLIENT -q "REVOKE SELECT(t) ON ${CLICKHOUSE_DATABASE}.t_sub_access FROM ${USER}"
run_as_user "select t.a after revoke (denied)" "SELECT t.a FROM ${CLICKHOUSE_DATABASE}.t_sub_access"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_sub_access"
$CLICKHOUSE_CLIENT -q "DROP USER ${USER}"
