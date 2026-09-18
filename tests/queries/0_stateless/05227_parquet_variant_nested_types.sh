#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Values nested inside a variant object keep their own type, and object/array offsets wider than
# one signed byte are read as unsigned.
DATA_FILE=$CUR_DIR/data_parquet/05227_variant_nested_types.parquet

${CLICKHOUSE_LOCAL} --query "
    SELECT n, arrayMap(x -> dynamicType(x), mapValues(dynamicElement(v, 'Map(String, Dynamic)'))) AS nested_types
    FROM file('${DATA_FILE}', Parquet)
    ORDER BY n
    SETTINGS session_timezone = 'UTC'
"

echo '--- values ---'
${CLICKHOUSE_LOCAL} --query "
    SELECT n, v
    FROM file('${DATA_FILE}', Parquet)
    WHERE n IN (2, 3, 4, 6)
    ORDER BY n
    SETTINGS session_timezone = 'UTC'
"
