#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.json

# Wrapper for clickhouse-client to always output in JSONEachRow format, that
# way format settings will not affect output.
function clickhouse_local()
{
    $CLICKHOUSE_LOCAL --output-format JSONEachRow "$@"
}

echo 'Array(UUID)'
echo '{"x":["00000000-0000-0000-0000-000000000000","b15f852c-c41a-4fd6-9247-1929c841715e",""]}' > $DATA_FILE
# Use increasingly smaller read buffers.
clickhouse_local -q "SELECT x FROM file('$DATA_FILE', 'JSONEachRow', 'x Array(UUID)') SETTINGS input_format_json_empty_as_default=1, input_format_parallel_parsing=0, storage_file_read_method='read', max_read_buffer_size=4"
clickhouse_local -q "SELECT x FROM file('$DATA_FILE', 'JSONEachRow', 'x Array(UUID)') SETTINGS input_format_json_empty_as_default=1, input_format_parallel_parsing=0, storage_file_read_method='read', max_read_buffer_size=2"
clickhouse_local -q "SELECT x FROM file('$DATA_FILE', 'JSONEachRow', 'x Array(UUID)') SETTINGS input_format_json_empty_as_default=1, input_format_parallel_parsing=0, storage_file_read_method='read', max_read_buffer_size=1"

echo 'Tuple(Array(UUID), Tuple(UUID, Map(String, IPv6)))'
echo '{"x":[[""], ["",{"abc":""}]]}' > $DATA_FILE
# Use increasingly smaller read buffers.
clickhouse_local -q "SELECT x FROM file('$DATA_FILE', 'JSONEachRow', 'x Tuple(Array(UUID), Tuple(UUID, Map(String, IPv6)))') SETTINGS input_format_json_empty_as_default=1, input_format_parallel_parsing=0, storage_file_read_method='read', max_read_buffer_size=16"
clickhouse_local -q "SELECT x FROM file('$DATA_FILE', 'JSONEachRow', 'x Tuple(Array(UUID), Tuple(UUID, Map(String, IPv6)))') SETTINGS input_format_json_empty_as_default=1, input_format_parallel_parsing=0, storage_file_read_method='read', max_read_buffer_size=8"
clickhouse_local -q "SELECT x FROM file('$DATA_FILE', 'JSONEachRow', 'x Tuple(Array(UUID), Tuple(UUID, Map(String, IPv6)))') SETTINGS input_format_json_empty_as_default=1, input_format_parallel_parsing=0, storage_file_read_method='read', max_read_buffer_size=1"

rm $DATA_FILE
