#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A shredded variant column: the group has a `typed_value` leaf next to `metadata` and `value`.
# Row 1 fits the shredded type and lives in `typed_value`; row 2 does not, so it falls back to the
# variant-encoded `value`. For any row exactly one of the two is non-null, so reading only `value`
# would silently lose row 1.
#
#   required group v {
#     required binary metadata;
#     optional binary value;
#     optional int32 typed_value;
#   }
DATA_FILE=$CUR_DIR/data_parquet/04930_variant_shredded.parquet

${CLICKHOUSE_LOCAL} --query="
    SELECT n, dynamicType(v) AS type, v
    FROM file('${DATA_FILE}', Parquet)
    ORDER BY n
"
