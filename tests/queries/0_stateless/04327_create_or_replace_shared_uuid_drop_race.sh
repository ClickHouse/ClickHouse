#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-ordinary-database, no-replicated-database

# Concurrent CREATE OR REPLACE TABLE queries that reuse one explicit UUID across distinct
# target names enqueue several dropped tables sharing that UUID. tables_marked_dropped_ids
# must track them per-occurrence; otherwise the second drop finalize hits chassert('removed').

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"
SHARED_UUID="00004327-0000-4000-8000-000043270001"

function cor_thread()
{
    local name="$1"
    for _ in {1..60}; do
        $CLICKHOUSE_CLIENT --database_atomic_wait_for_drop_and_detach_synchronously=0 \
            --query "CREATE OR REPLACE TABLE ${DB}.${name} UUID '${SHARED_UUID}' (x Int32) ENGINE = Memory" 2>/dev/null ||:
    done
}
export -f cor_thread

for n in {1..8}; do
    bash -c "cor_thread cor_$n" &
done
wait

# If the server crashed on the chassert, this final query fails and the test reports it.
$CLICKHOUSE_CLIENT --query "SELECT 1"

for n in {1..8}; do
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${DB}.cor_$n SYNC" 2>/dev/null ||:
done
