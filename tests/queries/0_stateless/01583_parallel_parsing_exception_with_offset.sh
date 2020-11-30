#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=none/g')

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS check;"

$CLICKHOUSE_CLIENT --query="CREATE TABLE check (x UInt64) ENGINE = Memory;"

(seq 1 2000000; echo 'hello'; seq 1 20000000) | $CLICKHOUSE_CLIENT --multiquery --query="SET input_format_parallel_parsing=1; SET min_chunk_bytes_for_parallel_parsing=100000; INSERT INTO check(x) FORMAT TSV " 2>&1 | grep -q "Offset: 1988984" && echo 'OK' || echo 'FAIL' ||:  

$CLICKHOUSE_CLIENT --query="DROP TABLE check;"
