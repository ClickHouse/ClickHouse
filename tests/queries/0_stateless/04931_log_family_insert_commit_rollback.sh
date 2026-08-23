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
