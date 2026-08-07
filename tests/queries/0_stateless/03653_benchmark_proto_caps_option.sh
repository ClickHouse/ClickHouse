#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_BENCHMARK --proto-caps bad_value -i 1 --query "SELECT 1" |& grep -v 'proto_caps option is incorrect (bad_value)'

$CLICKHOUSE_BENCHMARK --proto-caps notchunked -i 1 --query "SELECT 1" |& grep -F 'Queries executed'
$CLICKHOUSE_BENCHMARK --proto-caps recv_notchunked,send_notchunked -i 1 --query "SELECT 1" |& grep -F 'Queries executed'
$CLICKHOUSE_BENCHMARK --proto-caps recv_chunked,send_chunked,recv_chunked_optional,send_notchunked_optional -i 1 --query "SELECT 1" |& grep -F 'Queries executed'