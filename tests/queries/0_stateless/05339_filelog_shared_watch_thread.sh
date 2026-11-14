#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-parallel-replicas
# Tag no-fasttest: FileLog requires inotify.
# Tag no-parallel: the test compares the server-wide `GlobalThread` metric, which concurrent queries also change.
# Tag no-parallel-replicas: the test reads the `GlobalThread` metric of the server it is connected to.
#
# The directories of all `FileLog` tables are watched by one shared thread, so creating many tables must not
# create a thread per table, and dropping them while their directories change must not hang or break the server.
# They also share a fixed pool of inotify instances, so the number of tables is above the default
# `fs.inotify.max_user_instances` of 128.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLES=200
WORK="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
trap 'rm -rf "${WORK}"' EXIT
mkdir -p "${WORK}"

global_threads()
{
    $CLICKHOUSE_CLIENT -q "SELECT value FROM system.metrics WHERE metric = 'GlobalThread'"
}

table_name()
{
    printf '%s_%03d' "${CLICKHOUSE_TEST_UNIQUE_NAME}" "$1"
}

threads_before=$(global_threads)

for i in $(seq 1 "${TABLES}"); do
    mkdir -p "${WORK}/${i}"
    echo "pre_${i}" > "${WORK}/${i}/pre.tsv"
    echo "CREATE TABLE $(table_name "${i}") (v String) ENGINE = FileLog('${WORK}/${i}', 'TSV');"
done | $CLICKHOUSE_CLIENT -n

threads_after=$(global_threads)

# A thread per table would add at least ${TABLES} threads. The margin is generous for the threads of other queries.
echo "threads grew by less than the number of tables: $(( threads_after - threads_before < TABLES / 2 ))"

# The tables also read the files that appear after they were created, which is what the watcher delivers.
for i in 1 100 200; do
    echo "new_${i}" > "${WORK}/${i}/new.tsv"
done

# Direct reads consume the rows, so sum them: `pre.tsv` existed at creation, `new.tsv` comes through the watcher.
# A read before the watcher delivers the event may log the expected "no files need to parse" warning, so it is not sent to the client.
for i in 1 100 200; do
    total=0
    for _ in $(seq 1 600); do
        count=$($CLICKHOUSE_CLIENT --send_logs_level=error -q "SELECT count() FROM $(table_name "${i}") SETTINGS stream_like_engine_allow_direct_select = 1")
        total=$(( total + count ))
        [ "${total}" -ge 2 ] && break
        sleep 0.1
    done
    echo "table ${i}: ${total}"
done

# Drop all the tables while files keep appearing in their directories.
(
    for _ in $(seq 1 20); do
        for i in $(seq 1 "${TABLES}"); do
            echo "x" >> "${WORK}/${i}/pre.tsv" 2>/dev/null
        done
    done
) &
writer=$!

for i in $(seq 1 "${TABLES}"); do
    echo "DROP TABLE $(table_name "${i}") SYNC;"
done | $CLICKHOUSE_CLIENT -n

wait "${writer}"

# Two tables on the same directory share one inotify watch: dropping one must not stop the events of the other.
mkdir -p "${WORK}/shared"
$CLICKHOUSE_CLIENT -n -q "
    CREATE TABLE $(table_name 1001) (v String) ENGINE = FileLog('${WORK}/shared', 'TSV');
    CREATE TABLE $(table_name 1002) (v String) ENGINE = FileLog('${WORK}/shared', 'TSV');
    DROP TABLE $(table_name 1001) SYNC;
"
echo "shared" > "${WORK}/shared/new.tsv"
total=0
for _ in $(seq 1 600); do
    count=$($CLICKHOUSE_CLIENT --send_logs_level=error -q "SELECT count() FROM $(table_name 1002) SETTINGS stream_like_engine_allow_direct_select = 1")
    total=$(( total + count ))
    [ "${total}" -ge 1 ] && break
    sleep 0.1
done
echo "shared directory after drop: ${total}"
$CLICKHOUSE_CLIENT -q "DROP TABLE $(table_name 1002) SYNC"

$CLICKHOUSE_CLIENT -q "SELECT 'server is alive'"
