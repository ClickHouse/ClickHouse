#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings
# Tag no-fasttest: needs the urlCluster table function with an HTTP interface
# Tag no-random-settings: pins max_streams_for_files_processing_in_cluster_functions

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Fast-fail HTTP settings keep every stream's connection attempt instant so the queries below
# stay quick even after num_streams is clamped to a large-but-bounded value.
FAST="http_max_tries = 1, http_connection_timeout = 1, http_receive_timeout = 1, http_send_timeout = 1"

# Near-UINT64_MAX: used to throw std::length_error from pipes.reserve(num_streams) and abort.
${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM urlCluster('test_shard_localhost', 'http://127.0.0.1:9/', 'TSV', 'a UInt8')
    SETTINGS max_streams_for_files_processing_in_cluster_functions = 18446744073709551615, max_threads = 1, $FAST
" >/dev/null 2>&1

# Merely huge: reserve(num_streams) succeeded but pipe.resize(max_num_streams) then blew up memory.
${CLICKHOUSE_CLIENT} --query "
    SELECT count()
    FROM urlCluster('test_shard_localhost', 'http://127.0.0.1:9/', 'TSV', 'a UInt8')
    SETTINGS max_streams_for_files_processing_in_cluster_functions = 100000000, max_threads = 1, $FAST
" >/dev/null 2>&1

# The server must still be alive and responsive after both queries.
${CLICKHOUSE_CLIENT} --query "SELECT 'OK'"
