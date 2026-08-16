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

        $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS $tbl"
        $CLICKHOUSE_CLIENT -q "CREATE TABLE $tbl (a UInt64, arr Array(Array(Int64))) ENGINE = $engine"
        $CLICKHOUSE_CLIENT -q "INSERT INTO $tbl SELECT number, [[number, number + 1], [number + 2]] FROM numbers(100)"

        # Rows in [500, 560) belong to the insert that is made to fail, so they must never appear.
        $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $failpoint"
        $CLICKHOUSE_CLIENT -q "INSERT INTO $tbl SELECT number, [[number]] FROM numbers(500, 60)" 2>&1 \
            | grep -q -F "FAULT_INJECTED" && echo "failed insert was rejected: 1" || echo "failed insert was rejected: 0"
        $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $failpoint"

        # This insert reuses the metadata the rolled back one left behind. It must succeed, and only
        # its own rows may be added.
        $CLICKHOUSE_CLIENT -q "INSERT INTO $tbl SELECT number, [[number]] FROM numbers(9000, 40)"

        echo "rows of the failed insert: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM $tbl WHERE a >= 500 AND a < 560")"
        echo "rows: $($CLICKHOUSE_CLIENT -q "SELECT count(), sum(a) FROM $tbl")"
        echo "arrays readable: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM (SELECT a, arr FROM $tbl ORDER BY a DESC LIMIT 10) WHERE length(arr) > 0" 2>&1 | tail -1)"

        before=$($CLICKHOUSE_CLIENT -q "SELECT count(), sum(a) FROM $tbl")
        $CLICKHOUSE_CLIENT -q "DETACH TABLE $tbl"
        $CLICKHOUSE_CLIENT -q "ATTACH TABLE $tbl"
        after=$($CLICKHOUSE_CLIENT -q "SELECT count(), sum(a) FROM $tbl")
        if [ "$before" = "$after" ]; then
            echo "reload keeps the same data: 1"
        else
            echo "reload keeps the same data: 0 ($before vs $after)"
        fi

        echo "rows of the failed insert after reload: $($CLICKHOUSE_CLIENT -q "SELECT count() FROM $tbl WHERE a >= 500 AND a < 560")"

        $CLICKHOUSE_CLIENT -q "DROP TABLE $tbl"
    done
done
