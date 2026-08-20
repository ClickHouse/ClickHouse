#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, atomic-database
# no-fasttest: needs the `backups` disk, which the fast-test config does not define.
# no-replicated-database: creates its own databases and restores one under a new name.
# atomic-database: refreshable materialized views require an Atomic database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `RESTORE ... AS <new name>` rewrote every table reference in a stored refreshable MV definition
# except the `REFRESH ... DEPENDS ON` list, so a restored view kept a dependency on the source
# database. The refresh scheduler keys its dependency graph on the full name, so the restored view
# refreshed in response to a table it no longer read and ignored refreshes of its own parent.
#
# The oracle below is the persisted definition, not refresh timing: a narrow projection of
# `DEPENDS ON` / `TO` / `FROM` out of `create_table_query`, with the database names substituted to
# stable placeholders. No clock, no polling.

SRC="${CLICKHOUSE_DATABASE}_src"
DST="${CLICKHOUSE_DATABASE}_dst"
OUT="${CLICKHOUSE_DATABASE}_out"
BACKUP_DB="${CLICKHOUSE_TEST_UNIQUE_NAME}_db"
BACKUP_TBL="${CLICKHOUSE_TEST_UNIQUE_NAME}_tbl"

drop_all() {
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`$SRC\` SYNC"
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`$DST\` SYNC"
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`$OUT\` SYNC"
}

# An interrupted earlier run must not make this one fail with TABLE_ALREADY_EXISTS.
drop_all

# Print the renameable references of a view, with the database names replaced by placeholders so the
# reference is independent of $CLICKHOUSE_DATABASE.
show_refs() {
    local db="$1" view="$2"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT extract(create_table_query, 'DEPENDS ON ([^ ]+)') AS depends_on,
               extract(create_table_query, ' TO ([^ ]+)') AS to_target,
               extract(create_table_query, ' FROM ([^ ]+)') AS select_from
        FROM system.tables WHERE database = '$db' AND name = '$view' FORMAT TSV" \
    | sed -e "s/$SRC/SRC/g" -e "s/$DST/DST/g" -e "s/$OUT/OUT/g"
}

# A refreshable MV outside the backed-up set, used by the control arm below. `EVERY 1 YEAR` keeps it
# from refreshing on its own during the test.
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE \`$OUT\`"
${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW \`$OUT\`.p REFRESH EVERY 1 YEAR
    (a UInt64) ENGINE = MergeTree ORDER BY a AS SELECT 1 AS a"

${CLICKHOUSE_CLIENT} -q "CREATE DATABASE \`$SRC\`"
${CLICKHOUSE_CLIENT} --multiquery -q "
    CREATE TABLE \`$SRC\`.raw (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE \`$SRC\`.dst (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW \`$SRC\`.parent REFRESH EVERY 1 YEAR
        (a UInt64) ENGINE = MergeTree ORDER BY a AS SELECT a FROM \`$SRC\`.raw;
    CREATE MATERIALIZED VIEW \`$SRC\`.child REFRESH AFTER 1 SECOND DEPENDS ON \`$SRC\`.parent
        (a UInt64) ENGINE = MergeTree ORDER BY a AS SELECT a FROM \`$SRC\`.parent;
    CREATE MATERIALIZED VIEW \`$SRC\`.child_to REFRESH AFTER 1 SECOND DEPENDS ON \`$SRC\`.parent
        TO \`$SRC\`.dst AS SELECT a FROM \`$SRC\`.parent;
    CREATE MATERIALIZED VIEW \`$SRC\`.child_out REFRESH AFTER 1 SECOND DEPENDS ON \`$OUT\`.p
        (a UInt64) ENGINE = MergeTree ORDER BY a AS SELECT a FROM \`$SRC\`.raw;
"

${CLICKHOUSE_CLIENT} -q "BACKUP DATABASE \`$SRC\` TO Disk('backups', '$BACKUP_DB')" | grep -o "BACKUP_CREATED"
${CLICKHOUSE_CLIENT} -q "RESTORE DATABASE \`$SRC\` AS \`$DST\` FROM Disk('backups', '$BACKUP_DB')" | grep -o "RESTORED"

echo "1. inner-table view restored under a new database name:"
show_refs "$DST" child

echo "2. TO-target view restored under a new database name:"
show_refs "$DST" child_to

echo "3. control, dependency outside the restored set stays unchanged:"
show_refs "$DST" child_out

# The dependency must be rewritten exactly once and the pre-rename name must be gone entirely: a
# renamed reference appended next to the original one would leave both in the definition.
echo "4. renamed dependency appears once, source database name is gone:"
${CLICKHOUSE_CLIENT} -q "
    SELECT countMatches(create_table_query, 'DEPENDS ON ' || '$DST' || '\.parent') AS new_name,
           countMatches(create_table_query, '$SRC') AS old_name
    FROM system.tables WHERE database = '$DST' AND name = 'child' FORMAT TSV"

# Control: a per-table rename whose dependency is not itself renamed. The renaming map holds only
# child -> child2, so the dependency on `parent` must be left alone.
${CLICKHOUSE_CLIENT} -q "BACKUP TABLE \`$SRC\`.child TO Disk('backups', '$BACKUP_TBL')" | grep -o "BACKUP_CREATED"
${CLICKHOUSE_CLIENT} -q "RESTORE TABLE \`$SRC\`.child AS \`$SRC\`.child2 FROM Disk('backups', '$BACKUP_TBL')" | grep -o "RESTORED"

echo "5. control, table-level rename leaves a dependency that is not renamed:"
show_refs "$SRC" child2

drop_all
