#!/usr/bin/env bash
# Tags: log-engine

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A column of aggregate function states can occupy zero bytes on disk while the table has rows:
# countResample(10, 5, 1) covers an empty range, so every state serializes to nothing. A BACKUP of a
# Log family table must not read that as "the table has no rows", and a RESTORE must recreate the
# zero-byte data file instead of leaving the column's file missing.
#
# The row counts below are compared between the source and the restored table rather than against a
# literal: reading a column of empty states is a separate defect, so the absolute count changes when
# that is fixed, while the equality holds either way.

STATE="countResampleState(10, 5, 1)(number, number)"
COLUMN="AggregateFunction(countResample(10, 5, 1), UInt64, UInt64)"

backup() { echo "Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_$1')"; }

total_rows() {
    ${CLICKHOUSE_CLIENT} --query "
        SELECT '$2', ifNull(toString(total_rows), 'not counted') FROM system.tables
        WHERE database = currentDatabase() AND name = '$1'"
}

reads_as_source() {
    ${CLICKHOUSE_CLIENT} --query "
        SELECT 'restored s reads as source',
            (SELECT count() FROM $1 WHERE NOT ignore(s)) = (SELECT count() FROM $2 WHERE NOT ignore(s))"
}

echo '-- Log, the zero-byte column is the only data file'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t1 (s $COLUMN) ENGINE = Log;
    INSERT INTO t1 SELECT $STATE FROM numbers(100) GROUP BY number;"
total_rows t1 'source total_rows'
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t1 TO $(backup 1)" | grep -o BACKUP_CREATED
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE t1 AS t1r FROM $(backup 1)" | grep -o RESTORED
total_rows t1r 'restored total_rows'

echo '-- Log, a normal column comes first'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t2 (k UInt64, s $COLUMN) ENGINE = Log;
    INSERT INTO t2 SELECT number, $STATE FROM numbers(100) GROUP BY number;"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t2 TO $(backup 2)" | grep -o BACKUP_CREATED
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE t2 AS t2r FROM $(backup 2)" | grep -o RESTORED
${CLICKHOUSE_CLIENT} --query "SELECT 'restored sum(k)', sum(k) FROM t2r"
reads_as_source t2r t2

echo '-- restoring that backup onto the non-empty source table appends to it'
${CLICKHOUSE_CLIENT} --query "
    RESTORE TABLE t2 FROM $(backup 2) SETTINGS allow_non_empty_tables = 1" | grep -o RESTORED
total_rows t2 'total_rows after appending'

echo '-- TinyLog, a normal column comes first'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t3 (k UInt64, s $COLUMN) ENGINE = TinyLog;
    INSERT INTO t3 SELECT number, $STATE FROM numbers(100) GROUP BY number;"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t3 TO $(backup 3)" | grep -o BACKUP_CREATED
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE t3 AS t3r FROM $(backup 3)" | grep -o RESTORED
${CLICKHOUSE_CLIENT} --query "SELECT 'restored sum(k)', sum(k) FROM t3r"
reads_as_source t3r t3

# The oracle here is total_bytes, not a row count: TinyLog reads rows from the size of its first data
# file too, so both the source and the restored table read as empty and any count is the same either way.
echo '-- TinyLog, the zero-byte column comes first'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t6 (s $COLUMN, k UInt64) ENGINE = TinyLog;
    INSERT INTO t6 SELECT $STATE, number FROM numbers(100) GROUP BY number;"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t6 TO $(backup 6)" | grep -o BACKUP_CREATED
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE t6 AS t6r FROM $(backup 6)" | grep -o RESTORED
${CLICKHOUSE_CLIENT} --query "
    SELECT 'restored total_bytes matches source',
        (SELECT total_bytes FROM system.tables WHERE database = currentDatabase() AND name = 't6r')
      = (SELECT total_bytes FROM system.tables WHERE database = currentDatabase() AND name = 't6')"

echo '-- TinyLog, the zero-byte column alone: it keeps no row count on disk, so the rows are gone'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t4 (s $COLUMN) ENGINE = TinyLog;
    INSERT INTO t4 SELECT $STATE FROM numbers(100) GROUP BY number;"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t4 TO $(backup 4)" | grep -o BACKUP_CREATED
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE t4 AS t4r FROM $(backup 4)" | grep -o RESTORED
${CLICKHOUSE_CLIENT} --query "SELECT 'restored count()', count() FROM t4r"

echo '-- a Log table that was never inserted into carries no data to back up'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t5 (k UInt64) ENGINE = Log"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t5 TO $(backup 5)" | grep -o BACKUP_CREATED
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE t5 AS t5r FROM $(backup 5)" | grep -o RESTORED
${CLICKHOUSE_CLIENT} --query "SELECT 'restored count()', count() FROM t5r"
