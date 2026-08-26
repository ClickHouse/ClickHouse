#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: creates its own databases and restores one under a new name.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `RESTORE ... AS <new name>` rewrites the table references of a stored `CREATE` definition through a
# whitelist of AST shapes, and two entries were missing from it:
#
#  * of the `IN` operator family only the plain `IN` was rewritten, so `NOT IN`, `GLOBAL IN`, `nullIn`
#    and the rest kept reading the source database and the restored view returned the wrong rows; the
#    `IgnoreSet` counterparts kept a reference too, which breaks the view once the source is dropped;
#  * `ENGINE = Buffer(destination_database, destination_table, ...)` was not rewritten at all, so a
#    write to a restored buffer flushed into the source database.
#
# Only an identifier on the right-hand side of the family is a table. A string there is ordinary data,
# so rewriting one would corrupt a valid comparison; `v_literal` below is the control for that.
#
# The oracles below are the user-visible consequences rather than the definition text alone: what the
# restored views return, and which table a write to the restored buffer lands in.

SRC="${CLICKHOUSE_DATABASE}_src"
DST="${CLICKHOUSE_DATABASE}_dst"
OUT="${CLICKHOUSE_DATABASE}_out"
BACKUP="${CLICKHOUSE_TEST_UNIQUE_NAME}_db"
BACKUP_TBL="${CLICKHOUSE_TEST_UNIQUE_NAME}_tbl"

drop_all() {
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`$SRC\` SYNC"
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`$DST\` SYNC"
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`$OUT\` SYNC"
}

# These databases live outside $CLICKHOUSE_DATABASE, so the harness does not reclaim them: drop them
# on every exit path, not just the successful one.
trap drop_all EXIT

# An interrupted earlier run must not make this one fail with TABLE_ALREADY_EXISTS.
drop_all

# `$OUT` is never backed up, so the references pointing into it must survive the rename untouched.
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE \`$OUT\`"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE \`$OUT\`.u (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE \`$OUT\`.dest (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO \`$OUT\`.u VALUES (3);
"

# The buffer thresholds are high enough that nothing flushes on its own: only the explicit
# `OPTIMIZE` below does, so which table the row lands in does not depend on timing.
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE \`$SRC\`"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE \`$SRC\`.t (a UInt64) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE \`$SRC\`.u (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO \`$SRC\`.t VALUES (1), (2), (3);
    INSERT INTO \`$SRC\`.u VALUES (1);
    CREATE VIEW \`$SRC\`.v_in       AS SELECT a FROM \`$SRC\`.t WHERE a IN \`$SRC\`.u;
    CREATE VIEW \`$SRC\`.v_notin    AS SELECT a FROM \`$SRC\`.t WHERE a NOT IN \`$SRC\`.u;
    CREATE VIEW \`$SRC\`.v_globalin AS SELECT a FROM \`$SRC\`.t WHERE a GLOBAL IN \`$SRC\`.u;
    CREATE VIEW \`$SRC\`.v_nullin   AS SELECT a FROM \`$SRC\`.t WHERE nullIn(a, \`$SRC\`.u);
    CREATE VIEW \`$SRC\`.v_out      AS SELECT a FROM \`$SRC\`.t WHERE a NOT IN \`$OUT\`.u;
    CREATE VIEW \`$SRC\`.v_ignoreset AS SELECT a FROM \`$SRC\`.t WHERE inIgnoreSet(a, \`$SRC\`.u);
    CREATE VIEW \`$SRC\`.v_literal   AS SELECT '$SRC.u' NOT IN ('$SRC.u') AS r;
    CREATE TABLE \`$SRC\`.buf     (a UInt64) ENGINE = Buffer('$SRC', 't',    1, 3600, 3600, 1000000, 1000000, 100000000, 100000000);
    CREATE TABLE \`$SRC\`.buf_out (a UInt64) ENGINE = Buffer('$OUT', 'dest', 1, 3600, 3600, 1000000, 1000000, 100000000, 100000000);
"

${CLICKHOUSE_CLIENT} -q "BACKUP DATABASE \`$SRC\` TO Disk('backups', '$BACKUP')" | grep -o "BACKUP_CREATED"
${CLICKHOUSE_CLIENT} -q "RESTORE DATABASE \`$SRC\` AS \`$DST\` FROM Disk('backups', '$BACKUP')" | grep -o "RESTORED"

# Make the two databases disagree, so a view reading the wrong one returns different rows:
# `$SRC`.u becomes {1, 2} while the restored `$DST`.u stays {1}.
${CLICKHOUSE_CLIENT} -q "INSERT INTO \`$SRC\`.u VALUES (2)"

echo "1. restored view definitions:"
${CLICKHOUSE_CLIENT} -q "
    SELECT name, extract(create_table_query, 'WHERE.*')
    FROM system.tables WHERE database = '$DST' AND name LIKE 'v\_%'
    ORDER BY name FORMAT TSV" \
| sed -e "s/$SRC/SRC/g" -e "s/$DST/DST/g" -e "s/$OUT/OUT/g"

# `v_in` is the control arm: it was already rewritten, and it is one word away from `v_notin`.
# `v_out` is the second control: `$OUT` is not in the renaming map, so it must be left alone.
# `v_ignoreset` is absent here on purpose: an `IgnoreSet` variant ignores its right-hand side and
# always evaluates to the same constant, so its rows cannot tell a renamed reference from a stale one.
# Section 6 is its oracle instead.
echo "2. what the restored views return:"
for view in v_in v_notin v_globalin v_nullin v_out; do
    printf '%s\t' "$view"
    ${CLICKHOUSE_CLIENT} -q "SELECT arraySort(groupArray(a)) FROM \`$DST\`.$view"
done

echo "3. a write to the restored buffer lands in the restored destination:"
${CLICKHOUSE_CLIENT} -q "INSERT INTO \`$DST\`.buf VALUES (999)"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE \`$DST\`.buf"
printf 'SRC.t\t'
${CLICKHOUSE_CLIENT} -q "SELECT arraySort(groupArray(a)) FROM \`$SRC\`.t"
printf 'DST.t\t'
${CLICKHOUSE_CLIENT} -q "SELECT arraySort(groupArray(a)) FROM \`$DST\`.t"

echo "4. control, a buffer whose destination is outside the restored set keeps it:"
${CLICKHOUSE_CLIENT} -q "INSERT INTO \`$DST\`.buf_out VALUES (777)"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE \`$DST\`.buf_out"
printf 'OUT.dest\t'
${CLICKHOUSE_CLIENT} -q "SELECT arraySort(groupArray(a)) FROM \`$OUT\`.dest"

# The destination is spelled as a (database, table) pair that names one table, so it has to be looked
# up as one qualified name. A per-table rename moves the destination without touching its database,
# which is the case that resolving the two halves independently would get wrong.
# A string that merely looks like a table name is data, not a reference. Both sides are the same
# literal, so the comparison is false; rewriting only the right-hand side would flip it to true.
echo "4b. control, a database.table-shaped string on the right-hand side is left alone:"
printf 'v_literal\t'
${CLICKHOUSE_CLIENT} -q "SELECT r FROM \`$DST\`.v_literal"
printf 'definition\t'
${CLICKHOUSE_CLIENT} -q "SELECT extract(create_table_query, 'SELECT.*') FROM system.tables WHERE database = '$DST' AND name = 'v_literal' FORMAT TSV" \
| sed -e "s/$SRC/SRC/g" -e "s/$DST/DST/g"

echo "5. a table-level rename moves the buffer destination with it:"
${CLICKHOUSE_CLIENT} -q "BACKUP TABLE \`$SRC\`.t, TABLE \`$SRC\`.buf TO Disk('backups', '$BACKUP_TBL')" | grep -o "BACKUP_CREATED"
${CLICKHOUSE_CLIENT} -q "RESTORE TABLE \`$SRC\`.t AS \`$SRC\`.t2, TABLE \`$SRC\`.buf AS \`$SRC\`.buf2 FROM Disk('backups', '$BACKUP_TBL')" | grep -o "RESTORED"
${CLICKHOUSE_CLIENT} -q "INSERT INTO \`$SRC\`.buf2 VALUES (555)"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE \`$SRC\`.buf2"
printf 'SRC.t\t'
${CLICKHOUSE_CLIENT} -q "SELECT arraySort(groupArray(a)) FROM \`$SRC\`.t"
printf 'SRC.t2\t'
${CLICKHOUSE_CLIENT} -q "SELECT arraySort(groupArray(a)) FROM \`$SRC\`.t2"

# A reference the rename left behind is not inert even when it does not change any row: the analyzer
# resolves the right-hand side of every member of the family as a table expression and fails with
# UNKNOWN_IDENTIFIER when it is gone. So dropping the source database is the oracle that catches a
# stale reference in an `IgnoreSet` variant, whose rows never differ.
${CLICKHOUSE_CLIENT} -q "DROP DATABASE \`$SRC\` SYNC"
echo "6. the restored views still resolve once the source database is gone:"
for view in v_in v_notin v_globalin v_nullin v_ignoreset v_literal v_out; do
    printf '%s\t' "$view"
    if ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM \`$DST\`.$view" > /dev/null 2>&1; then
        echo "resolves"
    else
        echo "FAILS"
    fi
done
