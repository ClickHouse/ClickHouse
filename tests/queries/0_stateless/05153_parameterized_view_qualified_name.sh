#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --multiquery "
CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t SELECT arrayJoin([1, 2, 3]);
CREATE VIEW pv AS SELECT x FROM t WHERE x = {p:UInt64};
"

# A database-qualified reference to a parameterized view must resolve both when the dot qualifies
# the name and when the whole name arrives inside one quoted identifier: an AST round-trip (view
# metadata, `ON CLUSTER` DDL, a query shipped to another server) collapses `db.pv` into the single
# quoted token `db.pv`.
echo "--- resolution"
$CLICKHOUSE_CLIENT -q "SELECT * FROM ${DB}.pv(p = 2)"
$CLICKHOUSE_CLIENT -q "SELECT * FROM \`${DB}.pv\`(p = 2)"

# `EXPLAIN SYNTAX` inlines the view itself, so it has to split the name the same way execution
# does; both spellings have to render the same plan.
echo "--- explain syntax"
$CLICKHOUSE_CLIENT -q "EXPLAIN SYNTAX SELECT * FROM ${DB}.pv(p = 2)" | sed "s/${DB}/db/g"
$CLICKHOUSE_CLIENT -q "EXPLAIN SYNTAX SELECT * FROM \`${DB}.pv\`(p = 2)" | sed "s/${DB}/db/g"

# Formatting keeps the qualification: the database and the view are quoted separately, so the
# reference does not degrade into a reference to a table named `db.pv` on the way through metadata.
echo "--- formatting"
$CLICKHOUSE_CLIENT -q "SELECT replaceAll(formatQuerySingleLine('SELECT * FROM ${DB}.pv(p = 2)'), '${DB}', 'db')"
$CLICKHOUSE_CLIENT -q "CREATE VIEW outer_view AS SELECT * FROM ${DB}.pv(p = 3)"
$CLICKHOUSE_CLIENT -q "SELECT replaceAll(create_table_query, '${DB}', 'db') FROM system.tables WHERE database = currentDatabase() AND name = 'outer_view'"
