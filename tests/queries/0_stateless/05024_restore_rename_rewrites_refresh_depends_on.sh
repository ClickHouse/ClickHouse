#!/usr/bin/env bash
# Tags: no-replicated-database, atomic-database
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
DST2="${CLICKHOUSE_DATABASE}_dst2"
OUT="${CLICKHOUSE_DATABASE}_out"
BACKUP_DB="${CLICKHOUSE_TEST_UNIQUE_NAME}_db"
BACKUP_AS="${CLICKHOUSE_TEST_UNIQUE_NAME}_as"
BACKUP_TBL="${CLICKHOUSE_TEST_UNIQUE_NAME}_tbl"

drop_all() {
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`$SRC\` SYNC"
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`$DST\` SYNC"
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`$DST2\` SYNC"
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`$OUT\` SYNC"
}

# These databases live outside $CLICKHOUSE_DATABASE, so the harness does not reclaim them: drop them
# on every exit path, not just the successful one.
trap drop_all EXIT

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
    | sed -e "s/$SRC/SRC/g" -e "s/$DST2/DST2/g" -e "s/$DST/DST/g" -e "s/$OUT/OUT/g"
}

# The views below are created `EMPTY` with their next refresh a year out, so none of them refreshes
# while a `BACKUP DATABASE` of them is scanning. A non-append refresh swaps its target through an
# EXCHANGE, which the scan reports as an inconsistency warning on stderr, and the harness fails any
# test that writes to stderr. The oracle reads only `DEPENDS ON` / `TO` / `FROM`, never the schedule.
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE \`$OUT\`"
${CLICKHOUSE_CLIENT} -q "CREATE MATERIALIZED VIEW \`$OUT\`.p REFRESH AFTER 1 YEAR
    (a UInt64) ENGINE = MergeTree ORDER BY a EMPTY AS SELECT 1 AS a"

${CLICKHOUSE_CLIENT} -q "CREATE DATABASE \`$SRC\`"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE \`$SRC\`.raw (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE \`$SRC\`.dst (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE MATERIALIZED VIEW \`$SRC\`.parent REFRESH AFTER 1 YEAR
        (a UInt64) ENGINE = MergeTree ORDER BY a EMPTY AS SELECT a FROM \`$SRC\`.raw;
    CREATE MATERIALIZED VIEW \`$SRC\`.child REFRESH AFTER 1 YEAR DEPENDS ON \`$SRC\`.parent
        (a UInt64) ENGINE = MergeTree ORDER BY a EMPTY AS SELECT a FROM \`$SRC\`.parent;
    CREATE MATERIALIZED VIEW \`$SRC\`.child_to REFRESH AFTER 1 YEAR DEPENDS ON \`$SRC\`.parent
        TO \`$SRC\`.dst EMPTY AS SELECT a FROM \`$SRC\`.parent;
    CREATE MATERIALIZED VIEW \`$SRC\`.child_out REFRESH AFTER 1 YEAR DEPENDS ON \`$OUT\`.p
        (a UInt64) ENGINE = MergeTree ORDER BY a EMPTY AS SELECT a FROM \`$SRC\`.raw;
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

# `BACKUP ... AS` renames while writing the backup, through a different pair of call sites than the
# restore path uses. The stored definition is already renamed, so restoring it needs no rename.
${CLICKHOUSE_CLIENT} -q "BACKUP DATABASE \`$SRC\` AS \`$DST2\` TO Disk('backups', '$BACKUP_AS')" | grep -o "BACKUP_CREATED"
${CLICKHOUSE_CLIENT} -q "RESTORE DATABASE \`$DST2\` FROM Disk('backups', '$BACKUP_AS')" | grep -o "RESTORED"

echo "5. renamed at backup time rather than at restore time:"
show_refs "$DST2" child
echo "6. control, dependency outside the backed-up set stays unchanged:"
show_refs "$DST2" child_out

# Control: a per-table rename whose dependency is not itself renamed. The renaming map holds only
# child -> child2, so the dependency on `parent` must be left alone.
${CLICKHOUSE_CLIENT} -q "BACKUP TABLE \`$SRC\`.child TO Disk('backups', '$BACKUP_TBL')" | grep -o "BACKUP_CREATED"
${CLICKHOUSE_CLIENT} -q "RESTORE TABLE \`$SRC\`.child AS \`$SRC\`.child2 FROM Disk('backups', '$BACKUP_TBL')" | grep -o "RESTORED"

echo "7. control, table-level rename leaves a dependency that is not renamed:"
show_refs "$SRC" child2
