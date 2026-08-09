#!/usr/bin/env bash
# Tags: no-parallel, no-shared-merge-tree, no-replicated-database
# no-parallel: waits on a server-wide failpoint pause; a concurrent run would steal it.
# no-shared-merge-tree, no-replicated-database: drives a plain MergeTree ALTER, and the
#   failpoint is process-local, so an extra replica would apply the ALTER unpaused.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP=mt_alter_pause_after_metadata_publish

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_hints;
    CREATE TABLE t_hints (key UInt8, id UInt64, s String)
    ENGINE = MergeTree ORDER BY id PARTITION BY key
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, serialization_info_version = 'basic';

    INSERT INTO t_hints SELECT 1, number, toString(tuple(1, 0, '1', '0', '')) FROM numbers(1000);
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"

# Parks the ALTER after the new column types are published and before the serialization-hints
# swap, with parts_lock held.
$CLICKHOUSE_CLIENT --query "
    ALTER TABLE t_hints MODIFY COLUMN s Tuple(UInt64, UInt64, String, String, String)
" &
alter_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"

# Started only now, so the sink reads the already-published new metadata. A select-insert is
# used rather than an inline-data one: the inline form hands the table hints to the input format,
# where a stale base-class hint would trip DataTypeTuple::getSerialization's assert_cast in the
# parser, before the commit path this test covers.
$CLICKHOUSE_CLIENT --query "
    INSERT INTO t_hints SELECT 2, number, tuple(1, 0, '1', '0', '') FROM numbers(10)
" &
insert_pid=$!

# A green result must not be explainable by the INSERT having finished before the ALTER
# reached the window, so require it to be observed still running while the ALTER is paused.
contending=0
for _ in $(seq 1 400); do
    if [ "$($CLICKHOUSE_CLIENT --query "
                SELECT count() FROM system.processes
                WHERE current_database = currentDatabase() AND query ILIKE 'INSERT INTO t_hints%'")" = "1" ]; then
        contending=1
        break
    fi
    sleep 0.05
done
echo "insert contending: $contending"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"

wait $alter_pid
wait $insert_pid

$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM t_hints;
    SELECT sum(s.1), sum(s.2), groupUniqArray(s.3), groupUniqArray(s.4) FROM t_hints;
    DROP TABLE t_hints;
"
