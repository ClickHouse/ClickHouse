#!/usr/bin/env bash
# Focused regression for the `db.table` source-name branch in
# `safe_short_name` (see PR #107449 discussion r3483004360):
# when two unaliased tables share the same table name across different
# databases, `qualifyColumnNodesWithProjectionNames` emits canonical
# projection names like `db1.t.f1`, and the short-name fallback's
# `TableNode` branch must accept `database_name + "." + table_name` as a
# valid source-name match — otherwise the outer query can't resolve the
# rightmost short name.
#
# Lives in `.sh` rather than the sibling `.sql` so we can derive unique
# database names from `${CLICKHOUSE_DATABASE}`; pure-`.sql` cross-database
# tests collide on `DATABASE_ALREADY_EXISTS` under the parallel/flaky
# checks unless they add `Tags: no-parallel`, which is discouraged.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_A="${CLICKHOUSE_DATABASE}_a"
DB_B="${CLICKHOUSE_DATABASE}_b"

$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS \`${DB_A}\`"
$CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS \`${DB_B}\`"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE \`${DB_A}\`"
$CLICKHOUSE_CLIENT --query "CREATE DATABASE \`${DB_B}\`"
$CLICKHOUSE_CLIENT --query "CREATE TABLE \`${DB_A}\`.t (f1 UInt8) ENGINE = Memory"
$CLICKHOUSE_CLIENT --query "CREATE TABLE \`${DB_B}\`.t (f1 UInt8) ENGINE = Memory"
$CLICKHOUSE_CLIENT --query "INSERT INTO \`${DB_A}\`.t VALUES (10)"
$CLICKHOUSE_CLIENT --query "INSERT INTO \`${DB_B}\`.t VALUES (20)"

$CLICKHOUSE_CLIENT --multiquery <<EOF
SET enable_analyzer = 1;
SET analyzer_enable_short_column_names_from_subquery = 1;
SET single_join_prefer_left_table = 0;

-- The inner subquery puts BOTH unaliased \`${DB_A}\`.t and \`${DB_B}\`.t in scope so the
-- analyzer keeps the qualifier alive in the canonical projection name (it would have
-- looked like \`${DB_A}.t.f1\`), but the projection list selects only one of them so
-- the outer short-name lookup \`f1\` is unambiguous and exercises the \`db.table\`
-- source-name match in \`safe_short_name\`. Without that branch the outer \`SELECT f1\`
-- returns \`UNKNOWN_IDENTIFIER\`; with it, the short name resolves and we get \`10\`.
SELECT '-- short-name f1 resolves via the new db.table source match:';
SELECT f1 FROM (
    SELECT \`${DB_A}\`.t.f1 FROM \`${DB_A}\`.t, \`${DB_B}\`.t
);

SELECT '-- canonical dotted form keeps working:';
SELECT \`${DB_A}.t.f1\` FROM (
    SELECT \`${DB_A}\`.t.f1 FROM \`${DB_A}\`.t, \`${DB_B}\`.t
);
EOF

$CLICKHOUSE_CLIENT --query "DROP TABLE \`${DB_A}\`.t"
$CLICKHOUSE_CLIENT --query "DROP TABLE \`${DB_B}\`.t"
$CLICKHOUSE_CLIENT --query "DROP DATABASE \`${DB_A}\`"
$CLICKHOUSE_CLIENT --query "DROP DATABASE \`${DB_B}\`"
