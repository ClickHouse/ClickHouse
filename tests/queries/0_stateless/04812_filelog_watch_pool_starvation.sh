#!/usr/bin/env bash
# Tags: long, no-fasttest
# Tag no-fasttest: FileLog requires inotify.
# Tag long: three arms each wait 2s for the watchers to arm and 10s for them to deliver events.
#
# The directory watch loop must not occupy a BackgroundSchedulePool slot: the pool caps
# concurrent tasks per type at background_schedule_pool_size * ...max_parallel_tasks_per_type_ratio,
# and the loop never returns, so past that cap a table's watch was never armed and files
# appearing after CREATE were never ingested.
#
# Every arm runs in its own `clickhouse local` with a pinned pool size and ratio, and asserts the
# pinned values took effect (otherwise a future default change would make the arms vacuous).

set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORK=$(cd "${CLICKHOUSE_TMP}" && pwd)/${CLICKHOUSE_TEST_UNIQUE_NAME}
trap 'rm -rf "${WORK}"' EXIT

# Runs $2 FileLog tables under pool size $3 / ratio $4, writing one new file into every watched
# directory after the tables exist, and prints one "<table> <count>" line per table. `file()`
# resolves relative paths against the working directory, so the arm runs with its cwd equal to
# --user_files_path to keep the writes inside the watched directories.
run_arm()
{
    local arm="$1" tables="$2" pool="$3" ratio="$4"
    local dir="${WORK}/${arm}"
    rm -rf "${dir}"
    mkdir -p "${dir}/data"

    local i name
    for i in $(seq 1 "${tables}"); do
        name=$(printf 't%02d' "${i}")
        mkdir -p "${dir}/dirs/${name}"
        echo "pre_${name}" > "${dir}/dirs/${name}/pre.tsv"
    done

    {
        for i in $(seq 1 "${tables}"); do
            name=$(printf 't%02d' "${i}")
            echo "CREATE TABLE fl_${name} (v String) ENGINE=FileLog('${dir}/dirs/${name}', 'TSV');"
        done
        # Assert the pinned server settings are in effect for this arm.
        echo "SELECT 'pinned', getServerSetting('background_schedule_pool_size'), getServerSetting('background_schedule_pool_max_parallel_tasks_per_type_ratio');"
        # CREATE TABLE returns once the watch thread is created, not once inotify_add_watch has
        # run, so let every watcher arm before the files appear. A starved table's watch is never
        # armed at all and nothing re-scans, so this cannot mask the failure under test.
        echo "SELECT sleepEachRow(1) FROM numbers(2) FORMAT Null;"
        for i in $(seq 1 "${tables}"); do
            name=$(printf 't%02d' "${i}")
            echo "INSERT INTO FUNCTION file('dirs/${name}/new.tsv', 'TSV', 'v String') VALUES ('new_${name}');"
        done
        # Give the watchers time to deliver the CREATE events. sleepEachRow is capped at 3s per
        # block, hence max_block_size=1.
        echo "SELECT sleepEachRow(1) FROM numbers(10) SETTINGS max_block_size = 1 FORMAT Null;"
        for i in $(seq 1 "${tables}"); do
            name=$(printf 't%02d' "${i}")
            echo "SELECT 'fl_${name}', count() FROM fl_${name} SETTINGS stream_like_engine_allow_direct_select = 1;"
        done
    } > "${dir}/queries.sql"

    (
        cd "${dir}"
        ${CLICKHOUSE_LOCAL} --path=data --queries-file "${dir}/queries.sql" -- \
            --user_files_path="${dir}" \
            --background_schedule_pool_size="${pool}" \
            --background_schedule_pool_initial_size="${pool}" \
            --background_schedule_pool_max_parallel_tasks_per_type_ratio="${ratio}"
    ) < /dev/null
}

# Each table sees its pre-existing file (from the initial directory scan) plus the file created
# afterwards, so every count must be 2. Before the fix the tables beyond the per-type cap saw only
# the pre-existing file and reported 1.

# pool 5 * ratio 0.8 = cap 4, five tables: the fifth is the one that used to starve.
echo '-- five tables, cap 4'
run_arm cap4 5 5 0.8

# Same five tables with the cap raised to 5 by the ratio alone. This is the discriminator: it was
# already green before the fix, so it attributes the arm above to the cap rather than to the
# fixture or to the number of threads.
echo '-- five tables, cap 5 (discriminator)'
run_arm cap5 5 5 1.0

# pool 10 * 0.8 = cap 8: before the fix exactly the ninth and tenth tables starved.
echo '-- ten tables, cap 8'
run_arm cap8 10 10 0.8
