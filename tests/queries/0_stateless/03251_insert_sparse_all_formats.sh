#!/usr/bin/env bash
# Tags: no-fasttest, long, no-msan, no-azure-blob-storage, no-random-settings
# no-azure-blob-storage: too slow
# no-msan: it is too slow
# no-random-settings: this test is already slow, and randomized settings make it slower

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

formats=$($CLICKHOUSE_CLIENT --query "
    SELECT name FROM system.formats
    WHERE is_input AND is_output AND name NOT IN ('Template', 'Npy', 'RawBLOB', 'ProtobufList', 'ProtobufSingle', 'Protobuf', 'LineAsString', 'GeoJSON')
    ORDER BY name FORMAT TSV
")

schema_registry="http://127.0.0.1:8081"
# Subject must be unique to avoid conflicts with other tests
avro_settings="output_format_avro_confluent_subject=test_subject_03251&format_avro_schema_registry_url=$schema_registry"

# `enable_parsing_to_custom_serialization` below takes its hints from the parts that already exist,
# so the table must not be empty when the first format is inserted, and the ratio must stay below
# `b`/`c`'s default-ratio of 1.0 or they are not sparse and there are no hints to take.
prev=$($CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_sparse_all_formats;
    CREATE TABLE t_sparse_all_formats (a UInt64, b UInt64, c String) ENGINE = MergeTree ORDER BY a
        SETTINGS ratio_of_defaults_for_sparse_serialization = 0.9;
    INSERT INTO t_sparse_all_formats(a) SELECT number FROM numbers(1000);
    SELECT throwIf(count() = 0 OR countIf(serialization_kind != 'Sparse') != 0,
                   'seed did not arm sparse hints')
        FROM system.parts_columns
        WHERE database = currentDatabase() AND table = 't_sparse_all_formats'
          AND active AND column IN ('b', 'c')
    FORMAT Null;
    SELECT sum(sipHash64(*)) FROM t_sparse_all_formats;
")

for format in $formats; do
    echo $format

    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&$avro_settings" -d "SELECT number AS a, 0::UInt64 AS b, '' AS c FROM numbers(1000) FORMAT $format" \
        | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+t_sparse_all_formats+FORMAT+$format&enable_parsing_to_custom_serialization=1&$avro_settings" --data-binary @-

    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&$avro_settings" -d "SELECT number AS a FROM numbers(1000) FORMAT $format" \
        | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+t_sparse_all_formats(a)+FORMAT+$format&enable_parsing_to_custom_serialization=1&$avro_settings" --data-binary @-

    # Every format appends the same 2000 rows, so every delta below is the same constant.
    # `UInt64 - UInt64` is `Int64`, which these sums overflow, hence the widening.
    read -r delta prev <<< "$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d \
        "SELECT toUInt64(toInt128(s) - toInt128($prev)), s FROM (SELECT sum(sipHash64(*)) AS s FROM t_sparse_all_formats)")"
    echo "$delta"
done
