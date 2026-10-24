#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no usearch

# DROP TABLE SYNC must cancel a merge that is building a vector similarity index in one long step
# (one block of 200000 rows), instead of waiting minutes for the index build to finish.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE tab
    (
        id Int32,
        vec Array(Float32),
        INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 64, 'f32', 200, 400)
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS merge_max_block_size = 10000000, merge_max_block_size_bytes = 10000000000;

    SYSTEM STOP MERGES tab;
    SET materialize_skip_indexes_on_insert = 0;
    INSERT INTO tab SELECT number, arrayMap(x -> rand(x) / 4294967295., range(64)) FROM numbers(100000);
    INSERT INTO tab SELECT number + 100000, arrayMap(x -> rand(x) / 4294967295., range(64)) FROM numbers(100000);
    SYSTEM START MERGES tab;
"

$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE tab FINAL" >/dev/null 2>&1 &
optimize_pid=$!

merge_started=0
for _ in {1..600}; do
    if [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.merges WHERE database = currentDatabase() AND table = 'tab' AND elapsed > 3")" != 0 ]; then
        merge_started=1
        break
    fi
    sleep 0.1
done

# DROP TABLE SYNC also waits for the server-wide queue of dropped tables, which a concurrent drop of another table
# can hold for a long time, so time only the cancellation of the merge: the OPTIMIZE that runs it returns then.
start=$(date +%s)
$CLICKHOUSE_CLIENT -q "DROP TABLE tab SYNC" &
drop_pid=$!
wait $optimize_pid
elapsed=$(( $(date +%s) - start ))
wait $drop_pid

if [ "$merge_started" -eq 0 ]; then
    echo "The merge did not run for more than 3 seconds"
elif [ "$elapsed" -lt 30 ]; then
    echo "OK"
else
    echo "Cancelling the merge took $elapsed seconds"
fi
