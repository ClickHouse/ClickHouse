#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An unshredded variant column with the three payload-carrying encodings of the variant spec: the
# `string` primitive, the `binary` primitive and a short string. ClickHouse has no type distinct
# from String for binary, so all three land in one String variant rather than splitting the column
# into two types; the bytes must survive verbatim, including a payload that is not valid UTF-8 and
# contains an embedded zero byte.
DATA_FILE=$CUR_DIR/data_parquet/04933_variant_binary_and_string.parquet

${CLICKHOUSE_LOCAL} --query="
    SELECT dynamicType(v) AS type, length(v::String) AS bytes, hex(v::String) AS payload
    FROM file('${DATA_FILE}', Parquet)"

echo "-- one variant for all three encodings"
${CLICKHOUSE_LOCAL} --query="
    SELECT uniqExact(dynamicType(v))
    FROM file('${DATA_FILE}', Parquet)"
