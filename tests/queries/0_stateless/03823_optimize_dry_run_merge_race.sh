#!/usr/bin/env bash
# Tags: no-parallel, no-shared-merge-tree
# no-parallel: uses a server-wide failpoint, so a dry run of another test could consume the pause.
# no-shared-merge-tree: SMT coordinates merges differently; the race is exercised on plain `MergeTree`.

# Race two `OPTIMIZE ... DRY RUN` over the same parts: the first pauses at a failpoint while holding
# its temporary merge directory, then the second runs. Both must succeed.
#
# Run with short part names and with a part name long enough that appending the unique token to it
# would push the temporary directory past the filename limit, even though the corresponding real
# merge still fits. Both must keep the temporary directories distinct AND stay within the limit.
#
# The length of the temporary directory a dry run reserves is pinned as well, in both directions: it
# must not grow with the part name, and it must stay close to the budget of the merge it simulates.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP="merge_task_pause_after_reserving_tmp_dir"

# The prefix every temporary merge directory carries, dry run or not.
TMP_MERGE_PREFIX="tmp_merge_"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
}
trap cleanup EXIT

# $1 - table name, $2 - first source part, $3 - second source part
function race_two_dry_runs()
{
    local table=$1 part1=$2 part2=$3

    # The first `DRY RUN` merge will pause right after reserving its temporary directory.
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"

    # First `DRY RUN` in the background: it reserves the temporary directory, then pauses.
    $CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $table DRY RUN PARTS '$part1', '$part2'" &
    local dry_run_pid=$!

    # Wait until the first `DRY RUN` is actually paused holding its temporary directory.
    $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"

    # Second `DRY RUN` on the same parts, so it computes the same result part. Without the fix it
    # reserves the same temporary directory a second time and hits a `LOGICAL_ERROR`; with the fix
    # each `DRY RUN` gets a unique suffix and both succeed.
    $CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $table DRY RUN PARTS '$part1', '$part2'"
    echo "second dry run ok"

    # Release the paused first `DRY RUN` and let it finish.
    $CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT $FP"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
    wait $dry_run_pid
    echo "first dry run ok"

    # Server must be alive; `DRY RUN` commits nothing, so the source data is unchanged.
    $CLICKHOUSE_CLIENT --query "SELECT 'server alive', count() FROM $table"

    # Both dry runs must have actually merged. Succeeding without merging - for example by noticing
    # that the directory is taken and returning early - would leave the output above unchanged, so
    # check each query's own merge counters rather than only its exit status.
    $CLICKHOUSE_CLIENT --query "
        SYSTEM FLUSH LOGS query_log;
        SELECT 'merged', ProfileEvents['MergeSourceParts'], ProfileEvents['MergeWrittenRows']
        FROM system.query_log
        WHERE type = 'QueryFinish' AND current_database = currentDatabase()
          AND query LIKE '%DRY RUN PARTS%$part1%'
        ORDER BY event_time_microseconds
        SETTINGS enable_parallel_replicas = 0
    "
}

# The shortest part name a partitioned table can produce, so the race is covered at both ends of the
# part-name range: the temporary directory no longer contains the part name, and this pins that its
# length cannot make the names collide again.
echo "-- shortest part names"

$CLICKHOUSE_CLIENT --query "
    SET optimize_on_insert = 0;

    DROP TABLE IF EXISTS t_dry_run_race_min;
    CREATE TABLE t_dry_run_race_min (p UInt8, key UInt64)
    ENGINE = MergeTree ORDER BY key PARTITION BY p
    SETTINGS min_bytes_for_wide_part = 0;

    SYSTEM STOP MERGES t_dry_run_race_min;

    INSERT INTO t_dry_run_race_min SELECT 0, number FROM numbers(3);
    INSERT INTO t_dry_run_race_min SELECT 0, number + 10 FROM numbers(3);

    SELECT 'source part name length', length(name)
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_dry_run_race_min' AND active
    ORDER BY name LIMIT 1;
"

race_two_dry_runs t_dry_run_race_min 0_1_1_0 0_2_2_0

$CLICKHOUSE_CLIENT --query "DROP TABLE t_dry_run_race_min"

echo "-- short part names"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_dry_run_race;
    CREATE TABLE t_dry_run_race (key UInt64)
    ENGINE = MergeTree ORDER BY key
    SETTINGS min_bytes_for_wide_part = 0;

    SYSTEM STOP MERGES t_dry_run_race;

    INSERT INTO t_dry_run_race SELECT number FROM numbers(3);   -- all_1_1_0
    INSERT INTO t_dry_run_race SELECT number FROM numbers(3);   -- all_2_2_0
"

race_two_dry_runs t_dry_run_race all_1_1_0 all_2_2_0

$CLICKHOUSE_CLIENT --query "DROP TABLE t_dry_run_race"

# The temporary directory of a dry run does not follow the part name, so its length is fixed. Compare
# it against the budget of the merge it simulates: a dry run must not claim much more of the filename
# limit than that merge already takes, or a `DRY RUN` would be the first thing to hit `ENAMETOOLONG`
# on a filesystem with a small `NAME_MAX`, where the corresponding real merge still fits. It cannot
# claim exactly the same budget: reusing the name of the real merge is what collides in the first
# place. `IMergeTreeDataPart::remove` renames the directory to `delete_tmp_<dir>` on removal, so that
# longer name is what actually has to fit.
echo "-- temporary directory budget"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_dry_run_budget;
    CREATE TABLE t_dry_run_budget (key UInt64)
    ENGINE = MergeTree ORDER BY key
    SETTINGS min_bytes_for_wide_part = 0;

    SYSTEM STOP MERGES t_dry_run_budget;

    INSERT INTO t_dry_run_budget SELECT number FROM numbers(3);   -- all_1_1_0
    INSERT INTO t_dry_run_budget SELECT number FROM numbers(3);   -- all_2_2_0
"

# `send_logs_level` as a query setting rather than a client option: the client refuses the option
# twice, and `CLICKHOUSE_CLIENT` already carries it in some CI configurations.
DRY_RUN_TMP_DIR=$($CLICKHOUSE_CLIENT \
    --query "OPTIMIZE TABLE t_dry_run_budget DRY RUN PARTS 'all_1_1_0', 'all_2_2_0' SETTINGS send_logs_level='trace'" 2>&1 \
    | grep -o 'tmp_merge_dr_[0-9a-f]*' | head -n 1)

# The budget to compare against: the real merge of the same two parts reserves `tmp_merge_` followed
# by the name of the part it produces, so merge that pair for real and measure that name. Taken from
# `system.parts` rather than from the log, because the merge itself runs in the background pool and
# its `trace` messages do not travel back to the client.
MERGE_TMP_DIR_LENGTH=$($CLICKHOUSE_CLIENT --query "
    SYSTEM START MERGES t_dry_run_budget;
    OPTIMIZE TABLE t_dry_run_budget FINAL SETTINGS optimize_throw_if_noop = 1;
    SELECT length('$TMP_MERGE_PREFIX') + length(name)
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_dry_run_budget' AND active
    ORDER BY name LIMIT 1
")

echo "real merge directory length $MERGE_TMP_DIR_LENGTH"
echo "dry run directory length ${#DRY_RUN_TMP_DIR}"
echo "dry run costs $(( ${#DRY_RUN_TMP_DIR} - MERGE_TMP_DIR_LENGTH )) bytes more than the merge it simulates"
# `IMergeTreeDataPart::remove` prepends `delete_tmp_` to both names alike, so the difference above is
# the whole cost. From this part name length on, a dry run is no more expensive than the real merge.
echo "break-even part name length $(( ${#DRY_RUN_TMP_DIR} - ${#TMP_MERGE_PREFIX} ))"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_dry_run_budget"

# Same race, but with a part name long enough that a unique token no longer fits after it: a part name
# is not length-capped, and `MergeTree` also prepends `delete_tmp_` when removing the directory, so
# there is far less room left than the filename limit suggests. The corresponding real merge still
# succeeds at this length (asserted below), so a `DRY RUN` failing here would be broken where an
# ordinary `OPTIMIZE` works. An implementation that appended the token to the part name would pass
# the races above and fail here with `ENAMETOOLONG`.
echo "-- long part names"

LONG_PARTITION=$(printf -- '-9223372036854775808-%.0s' $(seq 1 10))4294967295

$CLICKHOUSE_CLIENT --query "
    SET optimize_on_insert = 0;

    DROP TABLE IF EXISTS t_dry_run_race_long;
    CREATE TABLE t_dry_run_race_long (c0 Int64, c1 Int64, c2 Int64, c3 Int64, c4 Int64, c5 Int64, c6 Int64, c7 Int64, c8 Int64, c9 Int64, ip IPv4, key UInt64)
    ENGINE = MergeTree ORDER BY key PARTITION BY (c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, ip)
    SETTINGS min_bytes_for_wide_part = 0;

    SYSTEM STOP MERGES t_dry_run_race_long;

    INSERT INTO t_dry_run_race_long SELECT -9223372036854775808, -9223372036854775808, -9223372036854775808, -9223372036854775808, -9223372036854775808, -9223372036854775808, -9223372036854775808, -9223372036854775808, -9223372036854775808, -9223372036854775808, toIPv4('255.255.255.255'), number FROM numbers(3);
    INSERT INTO t_dry_run_race_long SELECT -9223372036854775808, -9223372036854775808, -9223372036854775808, -9223372036854775808, -9223372036854775808, -9223372036854775808, -9223372036854775808, -9223372036854775808, -9223372036854775808, -9223372036854775808, toIPv4('255.255.255.255'), number + 10 FROM numbers(3);
"

# Guard the fixture: the part name must leave less room than a unique token needs, or the race below
# silently stops covering the length hazard. Appending a unique token to the name would need more
# bytes on top of `tmp_merge_` + name than the reported room, which is what the 255-byte limit
# actually leaves once `delete_tmp_` is prepended on removal.
$CLICKHOUSE_CLIENT --query "
    SELECT 'source part name length', length(name), 255 - (length(name) + length('delete_tmp_tmp_merge_')) AS room_left
    FROM system.parts
    WHERE database = currentDatabase() AND table = 't_dry_run_race_long' AND active
    ORDER BY name LIMIT 1
"

race_two_dry_runs t_dry_run_race_long "${LONG_PARTITION}_1_1_0" "${LONG_PARTITION}_2_2_0"

# The real merge of the very same parts succeeds at this name length, which is what makes a failing
# `DRY RUN` a bug rather than a filesystem limit the user has to live with.
$CLICKHOUSE_CLIENT --query "
    SYSTEM START MERGES t_dry_run_race_long;
    OPTIMIZE TABLE t_dry_run_race_long FINAL SETTINGS optimize_throw_if_noop = 1;
    SELECT 'real merge ok', count() FROM t_dry_run_race_long;
"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_dry_run_race_long"
