#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: `Parquet` format is not supported in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TMP}/04135_parquet_variant_wrapper_paths_optional.parquet"

rm -f "$FILE"

$CLICKHOUSE_LOCAL -q "
    SET enable_json_type = 1;
    SET output_format_parquet_use_custom_encoder = 1;
    SET output_format_parquet_json_as_variant = 1;
    SET input_format_parquet_use_native_reader_v3 = 1;

    CREATE TABLE src (j JSON(a UInt64, ts DateTime64(6, 'UTC'))) ENGINE = Memory;
    INSERT INTO src
    SELECT CAST('{\"a\":1,\"ts\":\"2024-01-02 03:04:05.123456\"}', 'JSON(a UInt64, ts DateTime64(6, \\'UTC\\'))');

    SELECT *
    FROM src
    INTO OUTFILE '$FILE'
    FORMAT Parquet;"

python3 - <<PY
from pathlib import Path

path = Path("$FILE")
old = b"ClickHouse.variant_wrapper_paths"
new = b"ClickHouse.variant_wrapper_pathx"

data = path.read_bytes()
assert data.count(old) == 1, data.count(old)
path.write_bytes(data.replace(old, new))
PY

$CLICKHOUSE_LOCAL -q "
    SET enable_analyzer = 1;
    SET enable_json_type = 1;
    SET input_format_parquet_use_native_reader_v3 = 1;

    SELECT j.a, toString(j.ts)
    FROM file(
        '$FILE',
        Parquet,
        'j JSON(a UInt64, ts DateTime64(6, \\'UTC\\'))')
    FORMAT TSVRaw;"
