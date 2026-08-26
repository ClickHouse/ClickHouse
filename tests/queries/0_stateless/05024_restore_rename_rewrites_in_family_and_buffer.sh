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
#    and the rest kept reading the source database and the restored view returned the wrong rows;
#  * `ENGINE = Buffer(destination_database, destination_table, ...)` was not rewritten at all, so a
#    write to a restored buffer flushed into the source database.
#
# The oracles below are the user-visible consequences rather than the definition text alone: what the
# restored views return, and which table a write to the restored buffer lands in.

SRC="${CLICKHOUSE_DATABASE}_src"
DST="${CLICKHOUSE_DATABASE}_dst"
OUT="${CLICKHOUSE_DATABASE}_out"
BACKUP="${CLICKHOUSE_TEST_UNIQUE_NAME}_db"

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
