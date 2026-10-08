#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings
# Tag no-fasttest: Depends on url() table function with HTTP interface
# Tag no-random-settings: Requires exact settings to reproduce the race condition

# Test for a race condition in TCPHandler where multiple ClusterFunctionReadTaskCallback
# invocations could read from a canceled ReadBuffer. When one callback's read fails and
# cancels the buffer, the next callback acquiring callback_mutex must see stop_query=true
# before attempting to read. Without the fix, this triggers:
# "Logical error: 'ReadBuffer is canceled. Can't read from it.'"

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The URL queries a non-existent table, producing errors that cancel the ReadBuffer
# and trigger the race between concurrent ClusterFunctionReadTaskCallback invocations.
${CLICKHOUSE_CLIENT} --query "
    SELECT DISTINCT [1]
    FROM url('${CLICKHOUSE_URL}&query=SELECT+c1,c0+FROM+no_such_db.no_such_table+FORMAT+RawBLOB', 'RawBLOB', 'c1 LowCardinality(Bool), c0 UInt32')
    ORDER BY 1 ASC
    FORMAT Null
    SETTINGS
        max_streams_for_files_processing_in_cluster_functions = 3697,
        allow_experimental_parallel_reading_from_replicas = 1,
        cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
        parallel_replicas_for_cluster_engines = 1,
        skip_unavailable_shards = 1,
        allow_suspicious_low_cardinality_types = 1,
        table_function_remote_max_addresses = 33,
        partial_result_on_first_cancel = 1
" 2>&1 | tr '\n' ' ' | grep -q -v -F "Logical error" && echo "OK" || echo "FAIL"
