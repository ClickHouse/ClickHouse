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
for analyzer in 1 0; do
    echo "--- enable_analyzer = $analyzer"
    $CLICKHOUSE_CLIENT --enable_analyzer "$analyzer" -q "SELECT * FROM ${DB}.pv(p = 2)"
    $CLICKHOUSE_CLIENT --enable_analyzer "$analyzer" -q "SELECT * FROM \`${DB}.pv\`(p = 2)"
done

# Formatting keeps the qualification: the database and the view are quoted separately, so the
# reference does not degrade into a reference to a table named `db.pv` on the way through metadata.
echo "--- formatting"
$CLICKHOUSE_CLIENT -q "SELECT replaceAll(formatQuerySingleLine('SELECT * FROM ${DB}.pv(p = 2)'), '${DB}', 'db')"
$CLICKHOUSE_CLIENT -q "CREATE VIEW outer_view AS SELECT * FROM ${DB}.pv(p = 3)"
$CLICKHOUSE_CLIENT -q "SELECT replaceAll(create_table_query, '${DB}', 'db') FROM system.tables WHERE database = currentDatabase() AND name = 'outer_view'"
