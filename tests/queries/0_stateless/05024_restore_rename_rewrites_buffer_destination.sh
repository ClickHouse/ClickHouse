#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: creates its own databases and restores one under a new name.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `RESTORE ... AS <new name>` rewrites the table references of a stored `CREATE` definition through a
# whitelist of AST shapes, and `ENGINE = Buffer(destination_database, destination_table, ...)` was not
# in it. A restored buffer therefore kept flushing into the source database: restoring a backup under
# a new name to inspect it, and then writing to it, modified the original database.
#
# The oracle is where a write lands, not the definition text, because that is the consequence.

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

# `$OUT` is never backed up, so a buffer pointing into it must survive the rename untouched.
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE \`$OUT\`"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE \`$OUT\`.dest (a UInt64) ENGINE = MergeTree ORDER BY a"

# The thresholds are high enough that nothing flushes on a timer: only the explicit `OPTIMIZE` calls
# below flush, so which table a row lands in never depends on timing.
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE \`$SRC\`"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE \`$SRC\`.t (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO \`$SRC\`.t VALUES (1), (2), (3);
    CREATE TABLE \`$SRC\`.buf     (a UInt64) ENGINE = Buffer('$SRC', 't',    1, 3600, 3600, 1000000, 1000000, 100000000, 100000000);
    CREATE TABLE \`$SRC\`.buf_out (a UInt64) ENGINE = Buffer('$OUT', 'dest', 1, 3600, 3600, 1000000, 1000000, 100000000, 100000000);
"

${CLICKHOUSE_CLIENT} -q "BACKUP DATABASE \`$SRC\` TO Disk('backups', '$BACKUP')" | grep -o "BACKUP_CREATED"
${CLICKHOUSE_CLIENT} -q "RESTORE DATABASE \`$SRC\` AS \`$DST\` FROM Disk('backups', '$BACKUP')" | grep -o "RESTORED"

echo "1. a write to the restored buffer lands in the restored destination:"
${CLICKHOUSE_CLIENT} -q "INSERT INTO \`$DST\`.buf VALUES (999)"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE \`$DST\`.buf"
printf 'SRC.t\t'
${CLICKHOUSE_CLIENT} -q "SELECT arraySort(groupArray(a)) FROM \`$SRC\`.t"
printf 'DST.t\t'
${CLICKHOUSE_CLIENT} -q "SELECT arraySort(groupArray(a)) FROM \`$DST\`.t"

echo "2. control, a buffer whose destination is outside the restored set keeps it:"
${CLICKHOUSE_CLIENT} -q "INSERT INTO \`$DST\`.buf_out VALUES (777)"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE \`$DST\`.buf_out"
printf 'OUT.dest\t'
${CLICKHOUSE_CLIENT} -q "SELECT arraySort(groupArray(a)) FROM \`$OUT\`.dest"

# The destination is spelled as a (database, table) pair naming one table, so it has to be looked up
# as one qualified name. A table-level rename moves the destination without touching its database,
# which is the case that resolving the two arguments independently would get wrong: a database-only
# lookup finds no mapping, and the bare table argument carries no database to parse.
echo "3. a table-level rename moves the buffer destination with it:"
${CLICKHOUSE_CLIENT} -q "BACKUP TABLE \`$SRC\`.t, TABLE \`$SRC\`.buf TO Disk('backups', '$BACKUP_TBL')" | grep -o "BACKUP_CREATED"
${CLICKHOUSE_CLIENT} -q "RESTORE TABLE \`$SRC\`.t AS \`$SRC\`.t2, TABLE \`$SRC\`.buf AS \`$SRC\`.buf2 FROM Disk('backups', '$BACKUP_TBL')" | grep -o "RESTORED"
${CLICKHOUSE_CLIENT} -q "INSERT INTO \`$SRC\`.buf2 VALUES (555)"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE \`$SRC\`.buf2"
printf 'SRC.t\t'
${CLICKHOUSE_CLIENT} -q "SELECT arraySort(groupArray(a)) FROM \`$SRC\`.t"
printf 'SRC.t2\t'
${CLICKHOUSE_CLIENT} -q "SELECT arraySort(groupArray(a)) FROM \`$SRC\`.t2"

echo "4. the restored definitions:"
${CLICKHOUSE_CLIENT} -q "
    SELECT name, extract(create_table_query, 'Buffer\\(.*\\)')
    FROM system.tables WHERE database = '$DST' AND name LIKE 'buf%'
    ORDER BY name FORMAT TSV" \
| sed -e "s/$SRC/SRC/g" -e "s/$DST/DST/g" -e "s/$OUT/OUT/g"
