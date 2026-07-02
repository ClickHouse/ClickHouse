#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: relies on background merge execution

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# SYSTEM SYNC MERGES retires a scheduled merge from ManualMergeSelector once it has fully synced it.
# That retirement must clear every carrier the merge is tracked by, otherwise select() keeps seeing a
# stale entry it can never match and stops there, starving every later SCHEDULE MERGE on the table.
#
# The stale-entry case is reached when a scheduled merge is satisfied by a covering part that arrives
# without select() ever matching its source parts. Here we reproduce it deterministically on a plain
# table: two source parts are merged into one part first, then the SAME (now merged away, no longer
# local) parts are scheduled again. The merged part already covers them, so SYNC MERGES succeeds on
# coverage alone and select() never matches the sources. If retirement leaves that impossible entry
# behind, a following SCHEDULE MERGE of a real pair is never assigned and its SYNC MERGES times out.
# With the fix the entry is fully retired and the later merge runs (SYNC_OK).
TABLE="sm_starve_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS $TABLE SYNC;
    CREATE TABLE $TABLE (x UInt64) ENGINE = MergeTree ORDER BY x
    SETTINGS merge_selector_algorithm = 'Manual', old_parts_lifetime = 600;
    INSERT INTO $TABLE VALUES (1);
    INSERT INTO $TABLE VALUES (2);
    INSERT INTO $TABLE VALUES (3);
    INSERT INTO $TABLE VALUES (4);
"

# Block numbering is not assumed: take the four single-row parts in part order. p1/p2 are merged
# first; p3/p4 are the later real merge whose starvation the test checks for.
parts=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = '$TABLE' AND active ORDER BY min_block_number")
read -r p1 p2 p3 p4 <<< "$(echo "$parts" | tr '\n' ' ')"

# Merge p1 + p2 the normal way (select() matches, executes the merge). The result part covers p1/p2.
$CLICKHOUSE_CLIENT -q "SYSTEM SCHEDULE MERGE $TABLE PARTS '$p1', '$p2'"
$CLICKHOUSE_CLIENT --max_execution_time 30 -q "SYSTEM SYNC MERGES $TABLE"

merged=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '$TABLE' AND active AND min_block_number <= (SELECT min_block_number FROM system.parts WHERE database = currentDatabase() AND table = '$TABLE' AND name = '$p2') AND max_block_number >= (SELECT max_block_number FROM system.parts WHERE database = currentDatabase() AND table = '$TABLE' AND name = '$p2')")
[ "$merged" = "1" ] || echo "FAIL: expected p1/p2 covered by one merged part, got $merged"

# Schedule the SAME parts again. They are gone locally (covered by the merged part), so select() can
# never match them; SYNC MERGES returns on coverage alone and retires the scheduled merge. The
# stale-entry bug is triggered exactly here.
$CLICKHOUSE_CLIENT -q "SYSTEM SCHEDULE MERGE $TABLE PARTS '$p1', '$p2'"
$CLICKHOUSE_CLIENT --max_execution_time 30 -q "SYSTEM SYNC MERGES $TABLE"

# Now a real, matchable merge of two live parts. If the earlier retirement left the impossible entry
# behind, select() stops at it and never assigns this one, so SYNC MERGES times out. With the fix the
# selector is clean and this merge runs to completion.
$CLICKHOUSE_CLIENT -q "SYSTEM SCHEDULE MERGE $TABLE PARTS '$p3', '$p4'"
if $CLICKHOUSE_CLIENT --max_execution_time 30 -q "SYSTEM SYNC MERGES $TABLE" 2>/dev/null; then
    echo SYNC_OK
else
    echo SYNC_TIMEOUT
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE $TABLE SYNC"
