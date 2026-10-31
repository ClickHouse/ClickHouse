#!/usr/bin/env bash
# Tags: log-engine

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A column can hold rows while occupying zero bytes on disk: the element stream of an array column
# receives nothing when every array is empty, so s.bin stays empty next to a populated s.size0.bin.
# A BACKUP of a Log family table records that file with size zero, and a RESTORE has to recreate it;
# otherwise every read of the column fails on a file that is not there while the RESTORE reported
# success. Restoring onto a table that already holds rows must instead leave the existing file alone.

COLUMN="Array(UInt64)"
VALUE="[]"

backup() { echo "Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_$1')"; }

# NOT ignore(s) reads the whole column, including the empty element stream.
reads_back() {
    ${CLICKHOUSE_CLIENT} --query "SELECT '$2', count(), sum(length(s)) FROM $1 WHERE NOT ignore(s)"
}

echo '-- Log'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t1 (s $COLUMN) ENGINE = Log;
    INSERT INTO t1 SELECT $VALUE FROM numbers(100);"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t1 TO $(backup 1)" | grep -o BACKUP_CREATED
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE t1 AS t1r FROM $(backup 1)" | grep -o RESTORED
reads_back t1r 'restored s reads back'

echo '-- Log, next to a column that holds bytes'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t2 (k UInt64, s $COLUMN) ENGINE = Log;
    INSERT INTO t2 SELECT number, $VALUE FROM numbers(100);"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t2 TO $(backup 2)" | grep -o BACKUP_CREATED
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE t2 AS t2r FROM $(backup 2)" | grep -o RESTORED
${CLICKHOUSE_CLIENT} --query "SELECT 'restored sum(k)', sum(k) FROM t2r"
reads_back t2r 'restored s reads back'

echo '-- restoring that backup onto the non-empty source table appends to it'
${CLICKHOUSE_CLIENT} --query "
    RESTORE TABLE t2 FROM $(backup 2) SETTINGS allow_non_empty_tables = 1" | grep -o RESTORED
reads_back t2 'source s reads back'

echo '-- TinyLog, next to a column that holds bytes'
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t3 (k UInt64, s $COLUMN) ENGINE = TinyLog;
    INSERT INTO t3 SELECT number, $VALUE FROM numbers(100);"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t3 TO $(backup 3)" | grep -o BACKUP_CREATED
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE t3 AS t3r FROM $(backup 3)" | grep -o RESTORED
${CLICKHOUSE_CLIENT} --query "SELECT 'restored sum(k)', sum(k) FROM t3r"
reads_back t3r 'restored s reads back'

echo '-- a Log table that was never inserted into carries no data to back up'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t4 (k UInt64) ENGINE = Log"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t4 TO $(backup 4)" | grep -o BACKUP_CREATED
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE t4 AS t4r FROM $(backup 4)" | grep -o RESTORED
${CLICKHOUSE_CLIENT} --query "SELECT 'restored count()', count() FROM t4r"
