#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree, no-parallel
# Tag no-object-storage: the test uses a disk of its own
# Tag no-replicated-database: plain rewritable should not be shared between replicas
# Tag no-parallel: the test drops the in-memory metadata cache of a disk that other tests may use as well

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Metadata transactions of unrelated tables on a plain_rewritable disk commit concurrently.
# Several tables are written, merged, moved between and truncated at the same time; afterwards the in-memory metadata
# is dropped and reloaded from the storage, and CHECK TABLE verifies that every file every table refers to is in place.

DISK="local_plain_rewritable_03008"
TABLES=4
ITERATIONS=10
ROWS_PER_INSERT=100

for i in $(seq 1 ${TABLES}); do
    ${CLICKHOUSE_CLIENT} -m --query "
    DROP TABLE IF EXISTS t_${i} SYNC;
    DROP TABLE IF EXISTS t_${i}_moved SYNC;
    CREATE TABLE t_${i} (a UInt64, b String) ENGINE = MergeTree ORDER BY a SETTINGS disk = '${DISK}';
    CREATE TABLE t_${i}_moved (a UInt64, b String) ENGINE = MergeTree ORDER BY a SETTINGS disk = '${DISK}';
    "
done

function workload()
{
    local table="$1"
    for ((iteration = 0; iteration < ITERATIONS; ++iteration)); do
        ${CLICKHOUSE_CLIENT} -m --query "
        INSERT INTO ${table} SELECT number, toString(number) FROM numbers(${ROWS_PER_INSERT});
        INSERT INTO ${table} SELECT number, toString(number) FROM numbers(${ROWS_PER_INSERT}, ${ROWS_PER_INSERT});
        OPTIMIZE TABLE ${table} FINAL;
        ALTER TABLE ${table} MOVE PARTITION tuple() TO TABLE ${table}_moved;
        OPTIMIZE TABLE ${table}_moved FINAL;
        "
    done
}

for i in $(seq 1 ${TABLES}); do
    workload "t_${i}" &
done
wait

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP DISK METADATA CACHE ${DISK}"

for i in $(seq 1 ${TABLES}); do
    ${CLICKHOUSE_CLIENT} -m --query "
    SELECT '${i}', count(), sum(a) FROM t_${i}_moved;
    SELECT '${i}', count() FROM t_${i};
    CHECK TABLE t_${i}_moved SETTINGS check_query_single_value_result = 1;
    CHECK TABLE t_${i} SETTINGS check_query_single_value_result = 1;
    DROP TABLE t_${i} SYNC;
    DROP TABLE t_${i}_moved SYNC;
    "
done
