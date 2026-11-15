#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A shredded variant column: the group has a `typed_value` leaf next to `metadata` and `value`.
# Row 1 fits the shredded type and lives in `typed_value`; row 2 does not, so it falls back to the
# variant-encoded `value`. Shredding is not supported yet, so reading the column is rejected
# instead of silently losing row 1. The other columns of the file stay readable, and schema
# inference can skip the column.
#
#   required group v {
#     required binary metadata;
#     optional binary value;
#     optional int32 typed_value;
#   }
DATA_FILE=$CUR_DIR/data_parquet/04930_variant_shredded.parquet

echo '--- reading the column ---'
${CLICKHOUSE_LOCAL} --query="SELECT n, v FROM file('${DATA_FILE}', Parquet) ORDER BY n" 2>&1 \
    | grep -o "Parquet column v is a shredded variant.*not supported yet"

echo '--- reading the other column ---'
${CLICKHOUSE_LOCAL} --query="SELECT n FROM file('${DATA_FILE}', Parquet, 'n Int32') ORDER BY n"

echo '--- schema inference ---'
${CLICKHOUSE_LOCAL} --query="DESCRIBE file('${DATA_FILE}', Parquet)" 2>&1 \
    | grep -o "Parquet column v is a shredded variant.*not supported yet"
${CLICKHOUSE_LOCAL} --query="
    DESCRIBE file('${DATA_FILE}', Parquet)
    SETTINGS input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference = 1"
