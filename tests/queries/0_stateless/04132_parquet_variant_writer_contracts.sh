#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: `Parquet` format is not supported in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

OBJECT_FILE="${CLICKHOUSE_TMP}/04132_parquet_variant_writer_contracts_object.parquet"
JSON_FILE="${CLICKHOUSE_TMP}/04132_parquet_variant_writer_contracts_json.parquet"

rm -f "$OBJECT_FILE" "$JSON_FILE"

$CLICKHOUSE_LOCAL -q "
    SET use_variant_as_common_type = 1;
    SET output_format_parquet_use_custom_encoder = 1;
    SET input_format_parquet_use_native_reader_v3 = 1;

    CREATE TABLE src (d Dynamic) ENGINE = Memory;
    INSERT INTO src
    SELECT CAST(
        CAST((number, toDateTime64('2024-01-02 03:04:05.123456', 6, 'UTC')), 'Tuple(a UInt64, ts DateTime64(6, \\'UTC\\'))'),
        'Dynamic')
    FROM numbers(1);

    SELECT *
    FROM src
    INTO OUTFILE '$OBJECT_FILE'
    FORMAT Parquet;"

python3 - <<PY
import pyarrow.parquet as pq

row = pq.read_table("$OBJECT_FILE").to_pylist()[0]["d"]
print(f"object_root_value_is_null={row['value'] is None}")
print(f"object_a_value_is_null={row['typed_value']['a']['value'] is None}")
print(f"object_a_typed_value={row['typed_value']['a']['typed_value']}")
print(f"object_ts_value_is_null={row['typed_value']['ts']['value'] is None}")
print(f"object_ts_typed_value={row['typed_value']['ts']['typed_value'].isoformat()}")
PY

$CLICKHOUSE_LOCAL -q "
    SET output_format_parquet_use_custom_encoder = 1;
    SET input_format_parquet_use_native_reader_v3 = 1;

    CREATE TABLE src (j JSON(max_dynamic_paths=1)) ENGINE = Memory;
    INSERT INTO src SELECT CAST('{\"a\":18446744073709551615}', 'JSON(max_dynamic_paths=1)');

    SELECT *
    FROM src
    INTO OUTFILE '$JSON_FILE'
    FORMAT Parquet
    SETTINGS output_format_parquet_json_as_variant = 1;"

python3 - <<PY
import pyarrow.parquet as pq

row = pq.read_table("$JSON_FILE").to_pylist()[0]["j"]
column = pq.ParquetFile("$JSON_FILE").schema.column(3)
print(f"json_root_value_is_null={row['value'] is None}")
print(f"json_typed_value={row['typed_value']['a']['typed_value']}")
print(f"json_typed_column={column.physical_type} {column.logical_type}")
PY
