#!/usr/bin/env bash
# Tags: no-fasttest, long

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

formats=$($CLICKHOUSE_CLIENT --query "
    SELECT name FROM system.formats
    WHERE is_input AND is_output AND name NOT IN ('Template', 'Npy', 'RawBLOB', 'ProtobufList', 'ProtobufSingle', 'Protobuf', 'LineAsString')
    ORDER BY name FORMAT TSV
")

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_sparse_all_formats;
    CREATE TABLE t_sparse_all_formats (a UInt64, b UInt64, c String) ENGINE = MergeTree ORDER BY a;
"

for format in $formats; do
    echo $format
    $CLICKHOUSE_CLIENT --query "INSERT INTO t_sparse_all_formats(a) SELECT number FROM numbers(1000)"

    $CLICKHOUSE_CLIENT --query "SELECT number AS a, 0::UInt64 AS b, '' AS c FROM numbers(1000) FORMAT $format" \
        | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+t_sparse_all_formats+FORMAT+$format&enable_parsing_to_custom_serialization=1" --data-binary @-

    $CLICKHOUSE_CLIENT --query "SELECT number AS a FROM numbers(1000) FORMAT $format" \
        | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+t_sparse_all_formats(a)+FORMAT+$format&enable_parsing_to_custom_serialization=1" --data-binary @-

    $CLICKHOUSE_CLIENT --query "
        SELECT sum(sipHash64(*)) FROM t_sparse_all_formats;
        TRUNCATE TABLE t_sparse_all_formats;
    "
done
