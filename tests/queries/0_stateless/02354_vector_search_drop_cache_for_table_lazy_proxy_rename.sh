#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: the fast test build has no vector similarity indexes (no `usearch` library)
# no-parallel: a `RENAME` in an `Ordinary` database clears all caches of the server

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In a database with `lazy_load_tables`, the catalog can hold a proxy of the table instead of the `MergeTree` table itself.
# `RENAME` locks that proxy, and in an `Ordinary` database it rewrites the paths of the parts.
# `SYSTEM DROP VECTOR SIMILARITY INDEX CACHE FOR TABLE` reads those paths, so it must lock the proxy too.
# A slow query holds the proxy and a `RENAME` waits for it, so the statement must wait as well.

DB="${CLICKHOUSE_DATABASE_1}"
READER_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_reader"
RENAME_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_rename"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id IN ('$READER_ID', '$RENAME_ID') SYNC" > /dev/null 2>&1
    $CLICKHOUSE_CLIENT --query "DROP DATABASE IF EXISTS $DB" 2>/dev/null
}
trap cleanup EXIT

# `--send_logs_level fatal` hides the server's warning that the `Ordinary` engine is deprecated.
$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary 1 --send_logs_level fatal --query "
    DROP DATABASE IF EXISTS $DB;
    CREATE DATABASE $DB ENGINE = Ordinary SETTINGS lazy_load_tables = 1;

    CREATE TABLE $DB.tab
    (
        id Int32,
        vec Array(Float32),
        INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
    )
    ENGINE = MergeTree
    ORDER BY id;

    INSERT INTO $DB.tab SELECT number, [toFloat32(number), 0.0] FROM numbers(300);
"

# The `INSERT` loaded the table. Attach the database again to put the table behind a proxy.
$CLICKHOUSE_CLIENT --query "DETACH DATABASE $DB"
$CLICKHOUSE_CLIENT --query "ATTACH DATABASE $DB"
$CLICKHOUSE_CLIENT --query "SELECT 'engine before the read', engine, is_loaded FROM system.tables WHERE database = '$DB' AND name = 'tab'"

# A slow query that holds the proxy until it is killed.
$CLICKHOUSE_CLIENT --query_id "$READER_ID" --query "
    SELECT sum(sleepEachRow(0.2)) FROM $DB.tab
    SETTINGS max_block_size = 1, max_threads = 1, function_sleep_max_microseconds_per_block = 100000000,
        enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0
" > /dev/null 2>&1 &
reader_pid=$!

reader_started=0
for _ in $(seq 1 600); do
    if [ "$($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$READER_ID' AND read_rows > 0")" = 1 ]; then
        reader_started=1
        break
    fi
    sleep 0.1
done
echo "reader started: $reader_started"

# The `RENAME` waits for the slow query to release the proxy.
$CLICKHOUSE_CLIENT --query_id "$RENAME_ID" --query "RENAME TABLE $DB.tab TO $DB.tab_renamed" &
rename_pid=$!

# Retry until the `RENAME` is queued. From then on, the statement waits behind it and times out.
waited_for_rename=0
for _ in $(seq 1 100); do
    error=$($CLICKHOUSE_CLIENT --lock_acquire_timeout 1 --query "SYSTEM DROP VECTOR SIMILARITY INDEX CACHE FOR TABLE $DB.tab" 2>&1)
    if echo "$error" | grep -q "READ locking attempt on .* has timed out.*Possible deadlock avoided"; then
        waited_for_rename=1
        break
    elif [ -n "$error" ]; then
        echo "unexpected error: $(echo "$error" | head -n 1)"
        break
    fi
    sleep 0.1
done
echo "the statement waited for the rename: $waited_for_rename"

$CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$READER_ID' SYNC" > /dev/null
wait $reader_pid
wait $rename_pid

$CLICKHOUSE_CLIENT --query "
    SELECT 'tables after the rename', name FROM system.tables WHERE database = '$DB' ORDER BY name;
    SYSTEM DROP VECTOR SIMILARITY INDEX CACHE FOR TABLE $DB.tab_renamed;
"
