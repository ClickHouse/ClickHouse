#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database, no-shared-merge-tree
# no-fasttest: a failpoint is server-global state, so the test must run alone, and such tests are kept out of the fast test.
# no-parallel: the failpoint pauses `MOVE PARTITION` of all tables.
# no-replicated-database, no-shared-merge-tree: the failpoint is in plain `MergeTree`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_move_partition_pause_before_commit" 2>/dev/null
}
trap cleanup EXIT

# `MOVE PARTITION 1 TO TABLE t1` from `t2` reads the parts to move and later covers them with empty
# parts of the next level. A concurrent `REPLACE PARTITION 1 FROM t1` into `t2` must not remove those
# parts in between: the empty parts would be committed over the outdated parts and intersect the
# empty part that covers the drop range of `REPLACE`, so `t2` could not be attached:
# "Part 1_2_2_5 intersects next part 1_2_28_5. It is a bug or a result of manual intervention".

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t1;
    DROP TABLE IF EXISTS t2;
    CREATE TABLE t1 (p UInt64, k UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k SETTINGS old_parts_lifetime = 3600;
    CREATE TABLE t2 (p UInt64, k UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k SETTINGS old_parts_lifetime = 3600;
    SYSTEM STOP MERGES t1;
    SYSTEM STOP MERGES t2;
    INSERT INTO t1 VALUES (1, 1);
    INSERT INTO t2 VALUES (1, 2);
    INSERT INTO t2 VALUES (1, 3);
    SYSTEM ENABLE FAILPOINT mt_move_partition_pause_before_commit;
"

$CLICKHOUSE_CLIENT -q "ALTER TABLE t2 MOVE PARTITION 1 TO TABLE t1" &
move_pid=$!

$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_move_partition_pause_before_commit PAUSE"

replace_query_id="${CLICKHOUSE_DATABASE}_replace_$RANDOM"
$CLICKHOUSE_CLIENT --query_id "$replace_query_id" -q "ALTER TABLE t2 REPLACE PARTITION 1 FROM t1" &
replace_pid=$!

# Without the fix `REPLACE` finishes while `MOVE` is paused. With the fix it waits for `MOVE`.
# Give it time to get to the point where it would remove the parts.
while [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$replace_query_id'") == 0 ]] && kill -0 $replace_pid 2>/dev/null
do
    sleep 0.1
done
sleep 1

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_move_partition_pause_before_commit"
wait $move_pid
wait $replace_pid

$CLICKHOUSE_CLIENT -q "
    DETACH TABLE t2;
    ATTACH TABLE t2;
    SELECT 't1', k FROM t1 ORDER BY k;
    SELECT 't2', k FROM t2 ORDER BY k;
    DROP TABLE t1;
    DROP TABLE t2;
"
