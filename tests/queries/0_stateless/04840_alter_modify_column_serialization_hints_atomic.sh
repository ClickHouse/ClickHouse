#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database
# no-parallel: waits on a server-wide failpoint pause; a concurrent run would steal it.
# no-shared-merge-tree, no-replicated-database: process-local failpoint, an extra replica would
#   apply the ALTER unpaused.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FP=mt_alter_pause_after_metadata_publish

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
}
trap cleanup EXIT

# $1: table name, $2: ENGINE clause. Run once per publish site: plain MergeTree reaches
# StorageMergeTree::alter, ReplicatedMergeTree reaches setTableStructure via ALTER_METADATA.
run_case() {
    local tbl=$1 engine=$2

    $CLICKHOUSE_CLIENT --query "
        SET insert_keeper_fault_injection_probability = 0;
        DROP TABLE IF EXISTS $tbl SYNC;
        CREATE TABLE $tbl (key UInt8, id UInt64, s String)
        ENGINE = $engine ORDER BY id PARTITION BY key
        SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9, serialization_info_version = 'basic';

        INSERT INTO $tbl SELECT 1, number, toString(tuple(1, 0, '1', '0', '')) FROM numbers(1000);
    "

    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"

    # Parks the ALTER after the new column types are published and before the
    # serialization-hints swap, with parts_lock held.
    $CLICKHOUSE_CLIENT --query "
        ALTER TABLE $tbl MODIFY COLUMN s Tuple(UInt64, UInt64, String, String, String)
    " &
    local alter_pid=$!

    $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"

    # Started only now, so the sink reads the already-published new metadata. Select-insert, not
    # inline-data: the inline form hands the hints to the input format, where a stale base-class
    # hint trips DataTypeTuple::getSerialization's assert_cast before the commit path covered here.
    $CLICKHOUSE_CLIENT --query "
        INSERT INTO $tbl SELECT 2, number, tuple(1, 0, '1', '0', '') FROM numbers(10)
        SETTINGS insert_keeper_fault_injection_probability = 0
    " &
    local insert_pid=$!

    # A green result must not be explainable by the INSERT having finished before the ALTER
    # reached the window, so require it to be observed still running while the ALTER is paused.
    local contending=0
    for _ in $(seq 1 400); do
        if [ "$($CLICKHOUSE_CLIENT --query "
                    SELECT count() FROM system.processes
                    WHERE current_database = currentDatabase()
                      AND query ILIKE 'INSERT INTO $tbl%'")" = "1" ]; then
            contending=1
            break
        fi
        sleep 0.05
    done
    echo "$tbl insert contending: $contending"

    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"

    wait $alter_pid
    wait $insert_pid

    $CLICKHOUSE_CLIENT --query "
        SELECT count() FROM $tbl;
        SELECT sum(s.1), sum(s.2), groupUniqArray(s.3), groupUniqArray(s.4) FROM $tbl;
        DROP TABLE $tbl SYNC;
    "
}

run_case t_hints_rep "ReplicatedMergeTree('/clickhouse/tables/{database}/04840_rmt', 'r1')"
run_case t_hints "MergeTree"
