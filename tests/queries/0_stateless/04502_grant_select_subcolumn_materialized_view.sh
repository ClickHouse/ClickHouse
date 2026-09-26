#!/usr/bin/env bash
# Tags: no-replicated-database, no-async-insert
# Tag no-replicated-database: an explicit DEFINER cannot be used in a Replicated database.
# Tag no-async-insert: the test relies on the INSERT materializing into the view synchronously.

# Regression test: reading a subcolumn through a materialized view must require the same
# source-table grants as reading its parent column. The source-side access check in
# `StorageMaterializedView` runs under the view's definer and used to check the raw requested
# names (e.g. `t.a`), so a definer holding `GRANT SELECT(t)` on the source table was denied.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"
DEFINER="test_definer_04502_${DB}"
READER="test_reader_04502_${DB}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${DEFINER}, ${READER}"

$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.src_04502 (id UInt32, t Tuple(a UInt32, b String)) ENGINE = MergeTree ORDER BY id"

$CLICKHOUSE_CLIENT -q "CREATE USER ${DEFINER}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${READER}"

# The definer holds parent-column grants on the source table; reading the view checks the
# source table under the definer's context.
$CLICKHOUSE_CLIENT -q "GRANT SELECT(id, t) ON ${DB}.src_04502 TO ${DEFINER}"

$CLICKHOUSE_CLIENT -q "
CREATE MATERIALIZED VIEW ${DB}.mv_04502
ENGINE = MergeTree ORDER BY tuple()
DEFINER = ${DEFINER} SQL SECURITY DEFINER
AS SELECT id, t FROM ${DB}.src_04502
"

$CLICKHOUSE_CLIENT -q "INSERT INTO ${DB}.src_04502 VALUES (1, (10, 'hello'))"

$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${DB}.mv_04502 TO ${READER}"

# Reading the parent column worked even before the fix; the subcolumn reads used to fail
# with ACCESS_DENIED because the source table was checked for `t.a` instead of `t`.
echo "=== select t (parent column) ==="
$CLICKHOUSE_CLIENT --user "${READER}" -q "SELECT t FROM ${DB}.mv_04502"

echo "=== select t.a (subcolumn) ==="
$CLICKHOUSE_CLIENT --user "${READER}" -q "SELECT t.a FROM ${DB}.mv_04502"

echo "=== select t.b (subcolumn) ==="
$CLICKHOUSE_CLIENT --user "${READER}" -q "SELECT t.b FROM ${DB}.mv_04502"

# After revoking the parent grant from the definer, the subcolumn read must be denied again:
# the normalization must not weaken the source-side check.
$CLICKHOUSE_CLIENT -q "REVOKE SELECT(t) ON ${DB}.src_04502 FROM ${DEFINER}"

echo "=== select t.a after revoke (denied) ==="
$CLICKHOUSE_CLIENT --user "${READER}" -q "SELECT t.a FROM ${DB}.mv_04502" 2>&1 | grep -o 'ACCESS_DENIED' | head -1

$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.mv_04502"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${DB}.src_04502"
$CLICKHOUSE_CLIENT -q "DROP USER ${DEFINER}, ${READER}"
