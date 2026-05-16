#!/usr/bin/env bash
# Tags: race, no-parallel
# no-parallel: runs 20 concurrent ALTER workers for 30 s — too heavy to share the server
#              with other tests; under asan_ubsan the server hits the docker memory limit

# Regression test for a data race in DatabaseMemory::alterTable.
# alterTable() used to take a raw (non-cloned) pointer to the stored
# create_query AST, release the mutex, then mutate the AST in place.
# Concurrently, system.tables queries call getCreateTableQueryImpl()
# which clones that same AST — reading children while they are being mutated.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="test_04093_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB}"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${DB} ENGINE = Memory"

# Wide initial schema increases the race window during applyMetadataChangesToCreateQuery
COLS=$(seq 0 19 | awk '{printf "%scol%d String", (NR>1?", ":""), $1}')
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${DB}.t (${COLS}) ENGINE = Memory"

function alter_worker()
{
    local tid=$1
    local TIMELIMIT=$((SECONDS + TIMEOUT))
    local idx=0
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "ALTER TABLE ${DB}.t ADD COLUMN dyn_${tid}_${idx} UInt64" 2>/dev/null || true
        if [ "$idx" -gt 3 ]; then
            $CLICKHOUSE_CLIENT -q "ALTER TABLE ${DB}.t DROP COLUMN dyn_${tid}_$(( idx - 3 ))" 2>/dev/null || true
        fi
        idx=$(( idx + 1 ))
    done
}

function read_worker()
{
    local TIMELIMIT=$((SECONDS + TIMEOUT))
    while [ $SECONDS -lt "$TIMELIMIT" ]; do
        $CLICKHOUSE_CLIENT -q "SELECT create_table_query FROM system.tables WHERE database = '${DB}' AND name = 't'" > /dev/null 2>&1 || true
    done
}

TIMEOUT=30

# Keep worker count low so the cgroup memory limit (10 GB under asan_ubsan)
# is not exceeded by many concurrent clickhouse-client ASan processes.
for i in {1..10}; do
    alter_worker "$i" 2>/dev/null &
done

for _ in {1..10}; do
    read_worker 2>/dev/null &
done

wait

$CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS ${DB}"

echo 'OK'
