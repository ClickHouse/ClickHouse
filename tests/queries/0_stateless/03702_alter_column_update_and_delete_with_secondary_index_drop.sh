#!/bin/bash
# Tags: no-parallel-replicas, no-random-merge-tree-settings
# no-parallel-replicas: The EXPLAIN output is completely different

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS test_table;"

wait_for_mutations() {
    while : ;
    do
        count=$(${CLICKHOUSE_CLIENT} --query="SELECT count() FROM system.mutations WHERE database = '$CLICKHOUSE_DATABASE' AND table = 'test_table' and is_done = 0")
        [[ $count != 0 ]] || break
        sleep 0.1
    done
}

client() {
    # SET enable_analyzer=1; -- Different EXPLAIN output
    # SET use_query_condition_cache = 0; -- Need it because we rerun some queries (with different settings) and we want to execute the full analysis
    $CLICKHOUSE_CLIENT --echo --enable-analyzer=1 --use_query_condition_cache=0 --alter_sync=2 --mutations_sync=2 -q "$1"
}

declare -a table_settings=("min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage=0")

## loop through above array
for part_type_setting in "${table_settings[@]}"
do

    client """
    CREATE TABLE test_table
    (
        id UInt64,

        value String,
        INDEX idx_ip_set (value) TYPE set(0) GRANULARITY 1,
    )
    ENGINE = MergeTree()
    ORDER BY id
    PARTITION BY intDiv(id, 100)
    SETTINGS alter_column_secondary_index_mode = 'drop', ${part_type_setting}, index_granularity=8192, index_granularity_bytes=0;

    INSERT INTO test_table SELECT number, number FROM numbers(10);

    SYSTEM STOP MERGES test_table;
    SET alter_sync = 0, mutations_sync = 0;
    ALTER TABLE test_table DELETE WHERE value = '3';

    -- The mutation is not applied yet. Using the index is ok since we are reading 'stale' data (it's not lightweight deletes)
    SELECT 'Status after DELETE is issued, before merges happen, max_threads=1';
    SET max_threads = 1;
    EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = '3';
    SELECT count() FROM test_table WHERE value = '3';

    SELECT 'Status after DELETE is issued, before merges happen, max_threads=2;';
    SET max_threads = 2;
    EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = '3';
    SELECT count() FROM test_table WHERE value = '3';

    SET max_threads = DEFAULT;
    SYSTEM START MERGES test_table;
    """

    wait_for_mutations

    client """
    SELECT 'Status after DELETE is applied (Index was dropped)';
    EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = '3';
    SELECT count() FROM test_table WHERE value = '3';

    -- Force a table rewrite. Index should be added back
    OPTIMIZE TABLE test_table FINAL;
    SELECT 'Status after DELETE full table rewrite (should be the same)';
    EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = '3';
    SELECT count() FROM test_table WHERE value = '3';
    """

    # Ingest a part that will not be affected by the mutation, to confirm the indices there are kept
    client """
    INSERT INTO test_table SELECT number, number FROM numbers(1000, 10);

    EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = '2000';
    SELECT 'first', count() FROM test_table WHERE value = '2000';
    ALTER TABLE test_table DELETE WHERE value = '8';

    EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = '2000';
    SELECT 'second', count() FROM test_table WHERE value = '2000';
    """

    client """
    SYSTEM STOP MERGES test_table;
    SET alter_sync = 0, mutations_sync = 0;
    ALTER TABLE test_table UPDATE value = '3' WHERE value = '5';

    -- The mutation is not applied yet. Using the index is ok since we are reading 'stale' data (it's not lightweight updates)
    SELECT 'Status after UPDATE is issued, before merges happen, max_threads=1';
    SET max_threads = 1;
    EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = '5';
    SELECT count() FROM test_table WHERE value = '5';

    SELECT 'Status after UPDATE is issued, before merges happen, max_threads=2;';
    SET max_threads = 2;
    EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = '5';
    SELECT count() FROM test_table WHERE value = '5';

    SET max_threads = DEFAULT;
    SYSTEM START MERGES test_table;
    """

    wait_for_mutations

    client """
    SELECT 'Status after UPDATE is applied (Index was dropped)';
    EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = '5';
    SELECT count() FROM test_table WHERE value = '5';

    -- Force a table rewrite. Index should be added back
    OPTIMIZE TABLE test_table FINAL;
    SELECT 'Status after DELETE full table rewrite (should be the same)';
    EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = '5';
    SELECT count() FROM test_table WHERE value = '5';

    DROP TABLE IF EXISTS test_table SYNC;
    """
done
