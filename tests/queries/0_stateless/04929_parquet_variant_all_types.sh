#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# One unshredded variant column covering every primitive type id (0..20) of the Apache variant
# encoding and all four basic types: primitive, short string, object, array.
DATA_FILE=$CUR_DIR/data_parquet/04929_variant_all_types.parquet

${CLICKHOUSE_LOCAL} --query="
    SELECT n, dynamicType(v) AS type, v
    FROM file('${DATA_FILE}', Parquet)
    ORDER BY n
    SETTINGS session_timezone = 'UTC'
"
