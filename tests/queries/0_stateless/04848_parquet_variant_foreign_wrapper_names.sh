#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: `Parquet` format is not supported in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A shredded `VARIANT` group whose children are named `value` / `typed_value` is only
# provably a synthetic residual/typed wrapper when the `ClickHouse.variant_wrapper_paths`
# footer key says so. Without that footer key (a foreign writer), such names can be
# ordinary object keys; misclassifying them as a wrapper used to reject the file with
# `primitive payload must be BYTE_ARRAY` when the `value` child is a typed primitive.
#
# Simulate a foreign file by writing a shredded file with ClickHouse and then renaming
# the `ClickHouse.*` footer keys in place (same byte length keeps the footer valid).

DIR=${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}
mkdir -p "${DIR}"
trap 'rm -rf "${DIR}"' EXIT

CH_FILE="${DIR}/wrappers.parquet"
FOREIGN_FILE="${DIR}/foreign.parquet"

${CLICKHOUSE_LOCAL} -q "
INSERT INTO FUNCTION file('${CH_FILE}', Parquet, 'v JSON(max_dynamic_paths=0, \`a.value\` Int32, \`a.typed_value\` String)')
SELECT CAST(raw AS JSON(max_dynamic_paths=0, \`a.value\` Int32, \`a.typed_value\` String)) AS v
FROM values('raw String',
    ('{\"a\":{\"value\":1,\"typed_value\":\"x\"}}'),
    ('{\"a\":{\"value\":2,\"typed_value\":\"y\"}}'))
SETTINGS output_format_parquet_use_custom_encoder = 1, output_format_parquet_json_as_variant = 1, engine_file_truncate_on_insert = 1;
"

python3 -c "
data = open('${CH_FILE}', 'rb').read()
data = data.replace(b'ClickHouse.variant_type_hints', b'XlickHouse.variant_type_hints')
data = data.replace(b'ClickHouse.variant_wrapper_paths', b'XlickHouse.variant_wrapper_paths')
open('${FOREIGN_FILE}', 'wb').write(data)
"

echo '-- with ClickHouse footer metadata'
${CLICKHOUSE_LOCAL} -q "
SELECT v FROM file('${CH_FILE}', Parquet, 'v JSON') ORDER BY toInt64(v.a.value)
SETTINGS input_format_parquet_use_native_reader_v3 = 1;
"

echo '-- foreign file (no ClickHouse footer metadata)'
${CLICKHOUSE_LOCAL} -q "
SELECT v FROM file('${FOREIGN_FILE}', Parquet, 'v JSON') ORDER BY toInt64(v.a.value)
SETTINGS input_format_parquet_use_native_reader_v3 = 1;
"

echo '-- foreign file, extracted key values'
${CLICKHOUSE_LOCAL} -q "
SELECT JSONExtractInt(toString(v), 'a', 'value'), JSONExtractString(toString(v), 'a', 'typed_value')
FROM file('${FOREIGN_FILE}', Parquet, 'v JSON') ORDER BY 1
SETTINGS input_format_parquet_use_native_reader_v3 = 1;
"

echo '-- foreign file, schema inference'
${CLICKHOUSE_LOCAL} -q "
SELECT v FROM file('${FOREIGN_FILE}', Parquet) ORDER BY 1
SETTINGS input_format_parquet_use_native_reader_v3 = 1;
"
