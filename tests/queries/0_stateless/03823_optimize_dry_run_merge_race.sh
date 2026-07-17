#!/usr/bin/env bash
# Tags: no-parallel, no-shared-merge-tree, no-random-settings, no-random-merge-tree-settings
# no-parallel: uses a server-wide failpoint that affects all merges.
# no-shared-merge-tree: SMT coordinates merges differently; the race is exercised on plain MergeTree.
# no-random-*: the test pins part names and merge behavior that the randomizer would otherwise change.

# Race two OPTIMIZE ... DRY RUN over the same parts: the first pauses at a failpoint while
# holding its temporary merge directory, then the second runs. Both must succeed.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP="merge_task_pause_after_reserving_tmp_dir"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_dry_run_race;
    CREATE TABLE t_dry_run_race (key UInt64)
    ENGINE = MergeTree ORDER BY key
    SETTINGS min_bytes_for_wide_part = 0;

    SYSTEM STOP MERGES t_dry_run_race;

    INSERT INTO t_dry_run_race SELECT number FROM numbers(3);   -- all_1_1_0
    INSERT INTO t_dry_run_race SELECT number FROM numbers(3);   -- all_2_2_0

    -- The first DRY RUN merge will pause right after reserving tmp_merge_all_1_2_1.
    SYSTEM ENABLE FAILPOINT $FP;
"

# First DRY RUN in the background. It reserves tmp_merge_all_1_2_1, then pauses at the failpoint.
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_dry_run_race DRY RUN PARTS 'all_1_1_0', 'all_2_2_0'" &
dry_run_pid=$!

# Wait until the first DRY RUN is actually paused holding tmp_merge_all_1_2_1.
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"

# Second DRY RUN on the same parts. It computes the same result part all_1_2_1. Without the fix
# it reserves tmp_merge_all_1_2_1 a second time and hits a LOGICAL_ERROR; with the fix each
# DRY RUN uses a unique temporary directory suffix and both succeed.
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_dry_run_race DRY RUN PARTS 'all_1_1_0', 'all_2_2_0'"
echo "second dry run ok"

# Release the paused first DRY RUN and let it finish.
$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT $FP"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
wait $dry_run_pid
echo "first dry run ok"

# Server must be alive; DRY RUN commits nothing, source data unchanged.
$CLICKHOUSE_CLIENT --query "SELECT 'server alive', count() FROM t_dry_run_race"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_dry_run_race"
