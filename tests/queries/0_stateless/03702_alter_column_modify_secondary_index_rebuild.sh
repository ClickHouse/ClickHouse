#!/bin/bash
# Tags: no-parallel-replicas
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
    # SET use_skip_indexes_on_data_read = 0; -- Need it for proper explain
    # SET use_query_condition_cache = 0; -- Need it because we rerun some queries (with different settings) and we want to execute the full analysis
    $CLICKHOUSE_CLIENT --echo --enable-analyzer=1 --use_skip_indexes_on_data_read=0 --use_query_condition_cache=0 --mutations_sync=2 --alter_sync=2 -q "$1"
}

declare -a table_settings=("min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage=0" "min_bytes_for_full_part_storage ='1G'")

## loop through above array
for part_type_setting in "${table_settings[@]}"
do

    client """
    CREATE TABLE test_table
    (
        id UInt64,

        value String,
        INDEX idx_ip_set (value) TYPE set(0) GRANULARITY 1,

        -- We will test ALTER COLUMN that would modify the index to an incompatible type
        ip String,
        INDEX idx_ip_bloom (ip) TYPE ngrambf_v1(3, 512, 2, 0) GRANULARITY 1,
    )
    ENGINE = MergeTree()
    ORDER BY id
    PARTITION BY id
    SETTINGS alter_column_secondary_index_mode = 'rebuild', ${part_type_setting};

    INSERT INTO test_table VALUES (1, '10', '127.0.0.1'), (2, '20', '127.0.0.2'), (3, '300', '127.0.0.3');

    SELECT 'IP column tests';
    EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE ip = '127.0.0.1';
    SELECT count() FROM test_table WHERE ip = '127.0.0.1';
    -- If we try to modify the column to a type incompatible with the index it should always throw an error and forbid the ALTER,
    -- independently of the alter_column_secondary_index_mode setting.
    ALTER TABLE test_table MODIFY COLUMN ip IPv4; -- { serverError INCORRECT_QUERY }

    SELECT 'STRING TO UInt64 tests';
    EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = '300';
    SELECT count() FROM test_table WHERE value = '300';

    SYSTEM STOP MERGES test_table;
    SET alter_sync = 0, mutations_sync = 0;
    ALTER TABLE test_table MODIFY COLUMN value UInt64;
    -- At this point the index is incompatible with the old parts so it should be ignored
    SELECT 'Status after ALTER is issued, before merges happen, max_threads=1';
    SET max_threads = 1;
    EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = '300';
    SELECT count() FROM test_table WHERE value = '300';

    SELECT 'Status after ALTER is issued, before merges happen, max_threads=2;';
    SET max_threads = 2;
    EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = '300';
    SELECT count() FROM test_table WHERE value = '300';

    SET max_threads = DEFAULT;
    SYSTEM START MERGES test_table;
    """

    wait_for_mutations

    client """
    SELECT 'Status after ALTER is applied';
    EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = '300';
    SELECT count() FROM test_table WHERE value = '300';

    -- Force a table rewrite to confirm it's the same
    OPTIMIZE TABLE test_table FINAL;
    SELECT 'Status after ALTER full table rewrite (should be the same)';
    EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = '300';
    SELECT count() FROM test_table WHERE value = '300';

    -- Doing a MODIFY COLUMN TTL should not affect the index
    SELECT 'ALTER TTL';
    SYSTEM STOP MERGES test_table;
    SET alter_sync = 0, mutations_sync = 0;
    ALTER TABLE test_table MODIFY COLUMN value MODIFY SETTING max_compress_block_size = 1048576;

    SELECT 'Status after unrelated ALTER MODIFY COLUMN is issued, before merges happen';
    EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = '300';
    SELECT count() FROM test_table WHERE value = '300';

    SYSTEM START MERGES test_table;
    """

    wait_for_mutations

    client """
    SELECT 'Status after unrelated ALTER is applied';
    EXPLAIN indexes = 1 SELECT count() FROM test_table WHERE value = '300';
    SELECT count() FROM test_table WHERE value = '300';

    DROP TABLE IF EXISTS test_table SYNC;
    """
done
