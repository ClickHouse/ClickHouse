#!/usr/bin/env bash
# Tags: no-parallel
# The test changes the dedicated volume and uses a global failpoint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -euo pipefail

cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT merge_parts_collection_before_volume_recheck"
    $CLICKHOUSE_CLIENT -q "SYSTEM START MERGES ON VOLUME merge_volume_recheck.main"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS volume_stop_during_collection SYNC"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE volume_stop_during_collection (x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS storage_policy = 'merge_volume_recheck', max_bytes_to_merge_at_max_space_in_pool = 0, max_bytes_to_merge_at_min_space_in_pool = 0;
INSERT INTO volume_stop_during_collection VALUES (1);
INSERT INTO volume_stop_during_collection VALUES (2);
SYSTEM ENABLE FAILPOINT merge_parts_collection_before_volume_recheck;
"

$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE volume_stop_during_collection FINAL SETTINGS optimize_throw_if_noop = 0" &
optimize_pid=$!
timeout 30 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT merge_parts_collection_before_volume_recheck PAUSE"
$CLICKHOUSE_CLIENT --multiquery -q "
SYSTEM STOP MERGES ON VOLUME merge_volume_recheck.main;
SYSTEM DISABLE FAILPOINT merge_parts_collection_before_volume_recheck;
"
wait "$optimize_pid"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts
WHERE database = currentDatabase() AND table = 'volume_stop_during_collection' AND active"
$CLICKHOUSE_CLIENT --multiquery -q "
SYSTEM START MERGES ON VOLUME merge_volume_recheck.main;
OPTIMIZE TABLE volume_stop_during_collection FINAL;
SELECT count() FROM system.parts
WHERE database = currentDatabase() AND table = 'volume_stop_during_collection' AND active;
SELECT sum(x) FROM volume_stop_during_collection;
"
