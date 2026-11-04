#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: needs failpoints, which are only available in a full build.
# no-parallel: fails due to failpoint intersection. The failpoints are server wide, so a
# concurrent copy that has one enabled makes this copy's own setup inserts fail.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# An insert into a Log family table whose commit of the recorded file sizes fails must leave no
# trace: the rows it wrote must not become visible, the array columns must stay readable, the
# following insert must land, and the metadata held in memory must describe the same data as the
# files on disk, so reloading the table must not change what it contains.

for engine in Log TinyLog StripeLog; do
    for failpoint in file_checker_update_and_save_fail_reading_sizes file_checker_update_and_save_fail_persisting; do
        echo "--- $engine / $failpoint"
        tbl="t_${engine}_${failpoint}"

        $CLICKHOUSE_CLIENT -q "
            DROP TABLE IF EXISTS $tbl;
            DROP TABLE IF EXISTS ${tbl}_before;
            CREATE TABLE $tbl (a UInt64, arr Array(Array(Int64))) ENGINE = $engine;
            INSERT INTO $tbl SELECT number, [[number, number + 1], [number + 2]] FROM numbers(100);
            SYSTEM ENABLE FAILPOINT $failpoint"

        # Rows in [500, 560) belong to the insert that is made to fail, so they must never appear.
        # It stays on its own connection: a statement that throws ends the whole request, so
        # anything batched after it here would silently not run.
        $CLICKHOUSE_CLIENT -q "INSERT INTO $tbl SELECT number, [[number]] FROM numbers(500, 60)" 2>&1 \
            | grep -q -F "FAULT_INJECTED" && echo "failed insert was rejected: 1" || echo "failed insert was rejected: 0"

        # The second insert reuses the metadata the rolled back one left behind. It must succeed, and
        # only its own rows may be added. ${tbl}_before carries the pre-reload counts across the
        # DETACH, which drops every scalar the session holds.
        $CLICKHOUSE_CLIENT -q "
            SYSTEM DISABLE FAILPOINT $failpoint;
            INSERT INTO $tbl SELECT number, [[number]] FROM numbers(9000, 40);
            SELECT 'rows of the failed insert: ' || toString(count()) FROM $tbl WHERE a >= 500 AND a < 560;
            SELECT 'rows: ' || toString(count()), sum(a) FROM $tbl;
            -- The counts held in memory are read without a mark range, so they see a stale mark
            -- that a read split across streams can group away.
            SELECT 'rows the table reports: ' || ifNull(toString(total_rows), 'not counted') FROM system.tables WHERE database = currentDatabase() AND name = '$tbl';
            SELECT 'arrays readable: ' || toString(count()) FROM (SELECT a, arr FROM $tbl ORDER BY a DESC LIMIT 10) WHERE length(arr) > 0;
            CREATE TABLE ${tbl}_before ENGINE = Log AS SELECT count() AS c, sum(a) AS s FROM $tbl;
            DETACH TABLE $tbl;
            ATTACH TABLE $tbl;
            SELECT 'reload keeps the same data: ' || toString(toUInt8(
                (SELECT c FROM ${tbl}_before) = (SELECT count() FROM $tbl)
                AND (SELECT s FROM ${tbl}_before) = (SELECT sum(a) FROM $tbl)));
            SELECT 'rows of the failed insert after reload: ' || toString(count()) FROM $tbl WHERE a >= 500 AND a < 560;
            DROP TABLE $tbl;
            DROP TABLE ${tbl}_before"
    done
done

# RESTORE appends the backup's data to whatever the table already holds and commits the recorded
# file sizes through the same path as an insert, so a failed commit there must leave the table as
# it was and must not leave the counts held in memory describing more data than the files hold.
# The counters are the marks file for Log and the index file for StripeLog; TinyLog has neither.
for engine in Log StripeLog; do
    for failpoint in file_checker_update_and_save_fail_reading_sizes file_checker_update_and_save_fail_persisting; do
        echo "--- restore $engine / $failpoint"
        src="s_${engine}_${failpoint}"
        dst="d_${engine}_${failpoint}"
        backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_${engine}_${failpoint}.zip')"

        # The destination already holds rows, so the restore appends the backup's marks or indices
        # to the ones already recorded rather than starting from an empty table.
        $CLICKHOUSE_CLIENT -q "
            DROP TABLE IF EXISTS $src;
            DROP TABLE IF EXISTS $dst;
            DROP TABLE IF EXISTS ${dst}_before;
            CREATE TABLE $src (a UInt64, arr Array(Array(Int64))) ENGINE = $engine;
            INSERT INTO $src SELECT number, [[number, number + 1], [number + 2]] FROM numbers(500, 60);
            CREATE TABLE $dst (a UInt64, arr Array(Array(Int64))) ENGINE = $engine;
            INSERT INTO $dst SELECT number, [[number]] FROM numbers(9000, 40);
            BACKUP TABLE $src TO $backup FORMAT Null;
            SYSTEM ENABLE FAILPOINT $failpoint"

        # Rows in [500, 560) exist only in the backup, so they must never reach the destination.
        # The restore stays on its own connection: a statement that throws ends the whole request.
        $CLICKHOUSE_CLIENT -q "RESTORE TABLE $src AS $dst FROM $backup SETTINGS allow_non_empty_tables = 1" 2>&1 \
            | grep -q -F "FAULT_INJECTED" && echo "failed restore was rejected: 1" || echo "failed restore was rejected: 0"

        # The insert after it reuses the counts the rolled back restore left behind, so it is what
        # shows whether those counts still describe the files. ${dst}_before carries the counts
        # across the DETACH, which drops every scalar the session holds.
        $CLICKHOUSE_CLIENT -q "
            SYSTEM DISABLE FAILPOINT $failpoint;
            SELECT 'restored rows: ' || toString(count()) FROM $dst WHERE a >= 500 AND a < 560;
            INSERT INTO $dst SELECT number, [[number]] FROM numbers(7000, 20);
            SELECT 'rows: ' || toString(count()), sum(a) FROM $dst;
            -- The counts held in memory are read without a mark range, so they see a stale mark
            -- that a read split across streams can group away.
            SELECT 'rows the table reports: ' || ifNull(toString(total_rows), 'not counted') FROM system.tables WHERE database = currentDatabase() AND name = '$dst';
            SELECT 'arrays readable: ' || toString(count()) FROM (SELECT a, arr FROM $dst ORDER BY a DESC LIMIT 10) WHERE length(arr) > 0;
            CREATE TABLE ${dst}_before ENGINE = Log AS SELECT count() AS c, sum(a) AS s FROM $dst;
            DETACH TABLE $dst;
            ATTACH TABLE $dst;
            SELECT 'reload keeps the same data: ' || toString(toUInt8(
                (SELECT c FROM ${dst}_before) = (SELECT count() FROM $dst)
                AND (SELECT s FROM ${dst}_before) = (SELECT sum(a) FROM $dst)));
            SELECT 'restored rows after reload: ' || toString(count()) FROM $dst WHERE a >= 500 AND a < 560;
            DROP TABLE $src;
            DROP TABLE $dst;
            DROP TABLE ${dst}_before"
    done
done
