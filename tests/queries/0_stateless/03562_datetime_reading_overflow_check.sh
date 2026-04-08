#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t0"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t0
(
    c0 FixedString(643),
    c1 Int8,
    c2 Array(Nullable(UInt8)),
    c3 Decimal(70),
    c4 LowCardinality(UInt256),
    c5 DateTime
)
ENGINE = Memory
SETTINGS allow_suspicious_fixed_string_types = 1, allow_suspicious_low_cardinality_types = 1"

$CLICKHOUSE_CLIENT -q "INSERT INTO t0 (c5, c4, c3, c2, c1, c0) FROM INFILE '$CURDIR/data_csv/overflow_check.csv'
SETTINGS input_format_parallel_parsing = 0, max_read_buffer_size = 8, input_format_allow_errors_num = 30
FORMAT CSV"

$CLICKHOUSE_CLIENT -q "DROP TABLE t0"
