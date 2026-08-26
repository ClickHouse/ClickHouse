#!/usr/bin/env bash
# Tags: no-fasttest, long, no-msan, no-azure-blob-storage
# no-azure-blob-storage: too slow
# no-msan: it is too slow

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

formats=$($CLICKHOUSE_CLIENT --query "
    SELECT name FROM system.formats
    WHERE is_input AND is_output AND name NOT IN ('Template', 'Npy', 'RawBLOB', 'ProtobufList', 'ProtobufSingle', 'Protobuf', 'LineAsString')
    ORDER BY name FORMAT TSV
")

schema_registry="http://127.0.0.1:8081"
# Subject must be unique to avoid conflicts with other tests
avro_settings="output_format_avro_confluent_subject=test_subject_03251&format_avro_schema_registry_url=$schema_registry"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_sparse_all_formats;
    CREATE TABLE t_sparse_all_formats (a UInt64, b UInt64, c String) ENGINE = MergeTree ORDER BY a;
"

for format in $formats; do
    echo $format
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "INSERT INTO t_sparse_all_formats(a) SELECT number FROM numbers(1000)"

    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&$avro_settings" -d "SELECT number AS a, 0::UInt64 AS b, '' AS c FROM numbers(1000) FORMAT $format" \
        | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+t_sparse_all_formats+FORMAT+$format&enable_parsing_to_custom_serialization=1&$avro_settings" --data-binary @-

    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&$avro_settings" -d "SELECT number AS a FROM numbers(1000) FORMAT $format" \
        | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+t_sparse_all_formats(a)+FORMAT+$format&enable_parsing_to_custom_serialization=1&$avro_settings" --data-binary @-

    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "SELECT sum(sipHash64(*)) FROM t_sparse_all_formats"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d "TRUNCATE TABLE t_sparse_all_formats"
done
