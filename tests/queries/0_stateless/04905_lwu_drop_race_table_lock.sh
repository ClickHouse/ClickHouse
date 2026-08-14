#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree
# Tag no-replicated-database: the test needs its own Memory database, which a Replicated database
#   run cannot host, and DROP must reach the synchronous exclusive-lock path.
# Tag no-shared-merge-tree: SharedMergeTree does not honor
#   sleep_before_commit_local_part_in_replicated_table_ms, which this test uses as its window.

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A Memory database has no UUID, so DROP takes the table's exclusive lock and removes the data
# inline instead of deferring it to the background cleanup an Atomic database uses.
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE_1}" < /dev/null
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${CLICKHOUSE_DATABASE_1} ENGINE = Memory" < /dev/null

# Every DROP the test's logic depends on pins ignore_drop_queries_probability: the stress runner
# injects 0.2 and clickhouse-client --fake-drop (upgrade check) injects 1, and for a storage that
# keeps data on disk the injection returns success without dropping anything.
function setup_table()
{
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE_1}.t SYNC SETTINGS ignore_drop_queries_probability = 0;

        CREATE TABLE ${CLICKHOUSE_DATABASE_1}.t (id UInt64, c2 String)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04905_$1/', '1')
        ORDER BY id
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

        INSERT INTO ${CLICKHOUSE_DATABASE_1}.t SELECT number, 'a' FROM numbers(2);

        ALTER TABLE ${CLICKHOUSE_DATABASE_1}.t
            MODIFY SETTING sleep_before_commit_local_part_in_replicated_table_ms = 10000;
    " < /dev/null
}

# The DROP must land between the patch-part rename and the commit, so it waits for the sink to
# announce its pre-commit pause for this arm's query id. An arm that never reaches that window says
# so rather than reporting success. The poll pins enable_parallel_replicas: system.text_log has to
# be read on this server, not on a cluster of hosts that never logged the message.
function race_with_drop()
{
    local arm=$1
    shift
    # Unique per invocation: a run reusing this server must not match an earlier run's row.
    local qid="${CLICKHOUSE_DATABASE_1}_${arm}_${RANDOM}${RANDOM}"

    ${CLICKHOUSE_CLIENT} --query_id "$qid" "$@" < /dev/null > /dev/null 2>&1 &
    local updater=$!

    local observed=0
    local seen
    for _ in $(seq 1 150); do
        seen=$(${CLICKHOUSE_CLIENT} --query "
            SYSTEM FLUSH LOGS text_log;
            SELECT count() FROM system.text_log
            WHERE query_id = '$qid' AND event_date >= yesterday()
              AND message LIKE '%committing part patch-%'
              AND message LIKE '%triggered sleep_before_commit_local_part_in_replicated_table_ms%'
            SETTINGS max_rows_to_read = 0, enable_parallel_replicas = 0" < /dev/null 2>/dev/null) || seen=""
        case "$seen" in
            '' | *[!0-9]*) seen=0 ;;
        esac
        if [ "$seen" -gt 0 ]; then
            observed=1
            break
        fi
        # The updater has exited, so the window can no longer be entered.
        kill -0 "$updater" 2>/dev/null || break
        sleep 0.5
    done

    local drop=skipped
    if [ "$observed" -eq 1 ]; then
        if ${CLICKHOUSE_CLIENT} --query "
            DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE_1}.t SYNC
            SETTINGS ignore_drop_queries_probability = 0" < /dev/null > /dev/null 2>&1; then
            drop=ok
            # A drop that reported success without removing the table took no lock and proved
            # nothing, so it is reported as its own outcome instead of counting as a win.
            if [ "$(${CLICKHOUSE_CLIENT} --query "
                EXISTS ${CLICKHOUSE_DATABASE_1}.t
                SETTINGS enable_parallel_replicas = 0" < /dev/null 2>/dev/null)" != 0 ]; then
                drop=ignored
            fi
        else
            drop=failed
        fi
    fi

    local update=ok
    wait "$updater" 2>/dev/null || update=failed

    if [ "$observed" -eq 1 ]; then
        echo "$arm: update=$update drop=$drop"
    else
        echo "$arm: commit window never observed"
    fi
}

setup_table update
race_with_drop update --enable_lightweight_update 1 \
    --query "UPDATE ${CLICKHOUSE_DATABASE_1}.t SET c2 = 'xx' WHERE id = 1"

setup_table alter
race_with_drop alter --enable_lightweight_update 1 --alter_update_mode 'lightweight_force' \
    --query "ALTER TABLE ${CLICKHOUSE_DATABASE_1}.t UPDATE c2 = 'xx' WHERE id = 1"

# An Alias table resolves a different storage, so the update has to hold the target's lock as well.
setup_table alias
${CLICKHOUSE_CLIENT} --allow_experimental_alias_table_engine 1 --query "
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE_1}.a SYNC SETTINGS ignore_drop_queries_probability = 0;
    CREATE TABLE ${CLICKHOUSE_DATABASE_1}.a ENGINE = Alias('${CLICKHOUSE_DATABASE_1}', 't');
" < /dev/null
race_with_drop alias --allow_experimental_alias_table_engine 1 --enable_lightweight_update 1 \
    --query "UPDATE ${CLICKHOUSE_DATABASE_1}.a SET c2 = 'xx' WHERE id = 1"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${CLICKHOUSE_DATABASE_1}" < /dev/null
