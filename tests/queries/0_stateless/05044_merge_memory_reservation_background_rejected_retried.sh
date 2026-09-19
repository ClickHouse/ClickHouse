#!/usr/bin/env bash
# Regression test for the background admission path of the merge memory reservation
# (StorageMergeTree::selectPartsToMerge): while one ordinary background merge holds its reservation,
# a second ordinary background merge whose estimate does not fit under
# merges_mutations_memory_usage_soft_limit must be rejected by MergeMemoryReservation::tryReserve at
# selection time, and once the first reservation is released the rejected merge must be retried by the
# background scheduler and run to completion - all inside one server process, with no OPTIMIZE involved.
#
# The soft limit is sized between one and two reservations (1.5x a measured single-merge estimate), so
# the rejection happens in tryReserve itself rather than in the canEnqueueBackgroundTask pre-check:
# with one reservation R held, R < 1.5R keeps the pre-check open, and only the second tryReserve
# (R > 1.5R - R) fails. Both rejection sites increment MergesRejectedByMemoryLimit; under this limit the
# pre-check cannot fire, so a non-zero counter pins the tryReserve rejection.
#
# The first selected merge is parked on the plain_merge_task_pause_before_prepare failpoint, which holds
# the task - and therefore its reservation - after selection but before execution. Each measurement runs
# in its own clickhouse-local process against its own data directory, so the metric only ever reflects
# this test's merges.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every table in this test is created with the same shape, data and settings, so all their merge
# estimates are (near-)identical and a limit of 1.5x one estimate admits exactly one of them.
function create_table()
{
    local table="$1"
    echo "
        CREATE TABLE $table (k UInt64, v String)
        ENGINE = MergeTree ORDER BY k
        SETTINGS min_age_to_force_merge_seconds = 5, min_age_to_force_merge_on_partition_only = 1;

        SYSTEM STOP MERGES $table;
        INSERT INTO $table SELECT number, repeat('a', 100) FROM numbers(2000);
        INSERT INTO $table SELECT number, repeat('b', 100) FROM numbers(2000, 2000);
    "
}

# Measures the reservation of a single merge of such a table: the background merge is selected - and its
# estimate reserved - once the parts are older than min_age_to_force_merge_seconds, and parks on the
# failpoint while the metric is read (the same harness as 05023).
function reserved_for_one_merge()
{
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05044_merge_memory_reservation_measure_XXXXXX")

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "$(create_table t_measure)" < /dev/null

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;
        SELECT sleepEachRow(3) FROM numbers(3) SETTINGS max_block_size = 1 FORMAT Null;
        SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation';
        SELECT sleepEachRow(3) FROM numbers(2) SETTINGS max_block_size = 1 FORMAT Null;
        SELECT value FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation';
        SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare;
    " < /dev/null | sort -rn | head -1

    rm -rf "$data_dir"
}

# On a loaded CI machine the background selector can lag behind min_age_to_force_merge_seconds, so a zero
# measurement means the merge was not selected within the window - retry on fresh data.
function reserved_for_one_merge_with_retries()
{
    local result=0
    for _ in 1 2 3
    do
        result=$(reserved_for_one_merge)
        result=${result:-0}
        if [ "$result" -gt 0 ]; then break; fi
    done
    echo "$result"
}

# One run of the scenario under a limit that fits one reservation but not two. Two identical tables race
# for the single admission slot: whichever merge is selected first reserves and parks on the failpoint,
# and the other is then rejected by tryReserve on every retry (which table wins does not matter - the
# assertions are symmetric). After the failpoint is released the winner completes, its reservation is
# released, and the loser's next background retry must reserve and complete too.
function rejected_and_retried()
{
    local limit="$1"
    local data_dir
    data_dir=$(mktemp -d "${CLICKHOUSE_TMP}/05044_merge_memory_reservation_scenario_XXXXXX")

    # The data is created right before the observing process starts, so at its startup the parts are
    # younger than min_age_to_force_merge_seconds and the failpoint is armed before any merge is selected.
    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        $(create_table t_first)
        $(create_table t_second)
    " < /dev/null

    ${CLICKHOUSE_LOCAL} --path "$data_dir" -q "
        SYSTEM ENABLE FAILPOINT plain_merge_task_pause_before_prepare;

        -- One of the two merges is selected, reserves, and parks on the failpoint while these sleeps run;
        -- the other keeps being rejected by tryReserve on every background retry.
        SELECT sleepEachRow(3) FROM numbers(3) SETTINGS max_block_size = 1 FORMAT Null;

        -- Exactly one reservation is admitted under this limit: held (> 0), and not two (two would
        -- exceed the limit, and tryReserve never lets the total grow past it while one is held).
        SELECT 'while held:';
        SELECT value > 0 AND value <= $limit FROM system.metrics WHERE metric = 'MergesMutationsMemoryReservation';

        -- Give the losing merge a few more background retries, each rejected by tryReserve.
        SELECT sleepEachRow(3) FROM numbers(2) SETTINGS max_block_size = 1 FORMAT Null;
        SELECT value > 0 FROM system.events WHERE event = 'MergesRejectedByMemoryLimit';

        -- Neither merge has produced its part: the winner is parked, the loser is rejected.
        SELECT countIf(table = 't_first'), countIf(table = 't_second')
        FROM system.parts WHERE database = currentDatabase() AND active AND table IN ('t_first', 't_second');

        SYSTEM DISABLE FAILPOINT plain_merge_task_pause_before_prepare;

        -- The winner runs to completion and releases its reservation; the loser's next retry is then
        -- admitted and completes as well - still in this same server process, with no user query driving it.
        SELECT sleepEachRow(3) FROM numbers(6) SETTINGS max_block_size = 1 FORMAT Null;
        SELECT 'after release:';
        SELECT countIf(table = 't_first'), countIf(table = 't_second')
        FROM system.parts WHERE database = currentDatabase() AND active AND table IN ('t_first', 't_second');
        SELECT count() FROM t_first;
        SELECT count() FROM t_second;
    " -- --merges_mutations_memory_usage_soft_limit="$limit" < /dev/null

    rm -rf "$data_dir"
}

expected="while held:
1
1
2	2
after release:
1	1
4000
4000"

reservation=$(reserved_for_one_merge_with_retries)
limit=$((reservation * 3 / 2))

# The scenario has timing windows (selection lag, the loser's retry cadence), so on a mismatch retry the
# whole run on fresh data instead of stretching every window; a real regression fails all three runs.
result=""
for _ in 1 2 3
do
    result=$(rejected_and_retried "$limit")
    if [ "$result" == "$expected" ]; then break; fi
done
echo "$result"
