#!/usr/bin/env bash
# Tags: no-replicated-database, no-async-insert
# Tag no-replicated-database: an explicit DEFINER cannot be used in a Replicated database.
# Tag no-async-insert: the test relies on the INSERT materializing into the view synchronously.

# Regression test for the materialized-view source-side access check when a view output column
# reuses an existing source-column name for data derived from a *different* source column.
#
# The source-side check in `StorageMaterializedView` is name-based by design: reading view output
# column `a` requires `SELECT(a)` on the source table, regardless of which source column the inner
# SELECT actually reads. That contract predates subcolumns and applies to parent-column reads too.
# This test locks in that a subcolumn read (`a.b`) is authorized *identically* to its parent read
# (`a`): both require `SELECT(a)` on the source, and revoking `SELECT(a)` denies both — even though
# the exposed data is derived from `x` and the definer still holds `SELECT(x)`. Subcolumns must not
# open any access path the parent-column read does not already have.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"
DEFINER="test_definer_04512_${DB}"
READER="test_reader_04512_${DB}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${DEFINER}, ${READER}"

# Source table has a scalar column `a` and a separate column `x`. The view below reuses the name
# `a` for a tuple built from `x`.
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.src_04512 (id UInt32, a UInt8, x UInt8) ENGINE = MergeTree ORDER BY id"

$CLICKHOUSE_CLIENT -q "CREATE USER ${DEFINER}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${READER}"

# Full source grants: the inner SELECT reads `x` to populate the view, and the output name `a` is
# what the source-side read check is keyed on.
$CLICKHOUSE_CLIENT -q "GRANT SELECT(id, a, x) ON ${DB}.src_04512 TO ${DEFINER}"

# Output column `a` (a named tuple with element `b`) is derived from source column `x`, and reuses
# the name of the unrelated source column `a`.
$CLICKHOUSE_CLIENT -q "
CREATE MATERIALIZED VIEW ${DB}.mv_04512
ENGINE = MergeTree ORDER BY tuple()
DEFINER = ${DEFINER} SQL SECURITY DEFINER
AS SELECT id, CAST(tuple(x) AS Tuple(b UInt8)) AS a FROM ${DB}.src_04512
"

$CLICKHOUSE_CLIENT -q "INSERT INTO ${DB}.src_04512 VALUES (1, 5, 42)"

$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${DB}.mv_04512 TO ${READER}"

# With SELECT(a) on the source, both the subcolumn and the parent read succeed. The returned data
# is derived from `x` (42), exactly as the parent read exposes it too.
echo "=== select a.b (subcolumn, derived from x) ==="
$CLICKHOUSE_CLIENT --user "${READER}" -q "SELECT a.b FROM ${DB}.mv_04512"

echo "=== select a (parent column) ==="
$CLICKHOUSE_CLIENT --user "${READER}" -q "SELECT a FROM ${DB}.mv_04512"

# Revoke only SELECT(a) from the definer; SELECT(x) is kept. The source-side check is name-based on
# the output name `a`, so both reads must now be denied even though the data comes from `x`. This
# proves the subcolumn read is no weaker than the parent read and opens no new access path.
$CLICKHOUSE_CLIENT -q "REVOKE SELECT(a) ON ${DB}.src_04512 FROM ${DEFINER}"

echo "=== select a after revoke SELECT(a) (denied) ==="
$CLICKHOUSE_CLIENT --user "${READER}" -q "SELECT a FROM ${DB}.mv_04512" 2>&1 | grep -o 'ACCESS_DENIED' | head -1

echo "=== select a.b after revoke SELECT(a) (denied) ==="
$CLICKHOUSE_CLIENT --user "${READER}" -q "SELECT a.b FROM ${DB}.mv_04512" 2>&1 | grep -o 'ACCESS_DENIED' | head -1

$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.mv_04512"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.src_04512"
$CLICKHOUSE_CLIENT -q "DROP USER ${DEFINER}, ${READER}"
