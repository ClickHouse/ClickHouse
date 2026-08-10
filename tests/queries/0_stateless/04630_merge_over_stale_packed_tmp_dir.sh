#!/usr/bin/env bash
# Tags: no-object-storage, no-shared-merge-tree, no-encrypted-storage, no-random-detach
# The tags are needed because the test manipulates part directories directly on the local filesystem.
# no-random-detach: a random DETACH/ATTACH reloads the table and cleans up the planted stale
# tmp_merge_ directory before the merge sees it, so the expected reclaim warning is never logged.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_merge_over_stale_packed"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC"
# storage_policy is pinned to 'default' because the test copies the part directory with cp below.
# The no-object-storage tag does not protect stress runs: they flip the default MergeTree
# storage_policy to (cached) S3 or Azure in the server config without passing the corresponding
# flag to clickhouse-test. On a remote disk, cp duplicates the metadata files without incrementing
# the blob reference counts, so reclaiming the stale directory deletes blobs still referenced by
# the live part, and every later read of it fails with 'The specified key does not exist'.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE $TABLE (a UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_full_part_storage = 1073741824, storage_policy = 'default'"

# Two deterministic parts with distinguishable contents, so contamination would change the values below.
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES $TABLE"
$CLICKHOUSE_CLIENT --query "INSERT INTO $TABLE SELECT number, number FROM numbers(50)"
$CLICKHOUSE_CLIENT --query "INSERT INTO $TABLE SELECT number + 50, (number + 50) * 100 FROM numbers(50)"

# Simulate a leftover of an interrupted merge in tmp_merge_all_1_2_1, filled with the real contents of
# an existing packed part, so the merge would produce visibly wrong data if it seeded anything from it.
part_path=$($CLICKHOUSE_CLIENT --query "
    SELECT path FROM system.parts
    WHERE database = currentDatabase() AND table = '$TABLE' AND name = 'all_1_1_0' AND active")
table_data_path=$(dirname "${part_path%/}")
stale_dir="$table_data_path/tmp_merge_all_1_2_1"
cp -r "${part_path%/}" "$stale_dir"
if [ -f "$stale_dir/data.packed" ]
then
    echo "stale tmp_merge dir with data.packed created"
else
    echo "FAILED to create stale dir, part path was: $part_path"
    ls "$stale_dir" 2>&1
fi

# send_logs_level=error hides the expected reclaim warning.
$CLICKHOUSE_CLIENT --send_logs_level=error --multiquery --query "
SYSTEM START MERGES $TABLE;
OPTIMIZE TABLE $TABLE FINAL SETTINGS optimize_throw_if_noop = 1;
"

# Exactly the inserted data, in one active packed part that passes the consistency check.
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(a), sum(v) FROM $TABLE"
$CLICKHOUSE_CLIENT --query "SELECT a, v FROM $TABLE ORDER BY a LIMIT 2"
$CLICKHOUSE_CLIENT --query "SELECT a, v FROM $TABLE ORDER BY a DESC LIMIT 2"
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.parts
    WHERE database = currentDatabase() AND table = '$TABLE' AND active"
$CLICKHOUSE_CLIENT --query "
    SELECT DISTINCT part_storage_type FROM system.parts
    WHERE database = currentDatabase() AND table = '$TABLE' AND active"
$CLICKHOUSE_CLIENT --query "CHECK TABLE $TABLE SETTINGS check_query_single_value_result = 1"

# The stale directory was actually reclaimed (it existed at merge time).
found=0
for _ in {1..10}
do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    found=$($CLICKHOUSE_CLIENT --query "
        SELECT count() > 0 FROM system.text_log
        WHERE startsWith(logger_name, currentDatabase() || '.$TABLE')
          AND message LIKE '%Removing stale temporary directory%'
          AND message LIKE '%/tmp_merge_%'
    ")
    [[ $found == 1 ]] && break
    sleep 0.5
done

if [[ $found == 1 ]]
then
    echo "tmp_merge_ reclaim warning found"
else
    echo "tmp_merge_ reclaim warning NOT found, messages logged for the table:"
    $CLICKHOUSE_CLIENT --query "
        SELECT logger_name, message FROM system.text_log
        WHERE startsWith(logger_name, currentDatabase() || '.$TABLE') ORDER BY event_time_microseconds"
fi
