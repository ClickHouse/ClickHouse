#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Variant objects are read as `JSON` with nested objects flattened into paths and null fields
# dropped. Arrays of objects are read as `Array(JSON)`, other arrays as `Array(Dynamic)`.
DATA_FILE=$CUR_DIR/data_parquet/05296_variant_json_objects.parquet

${CLICKHOUSE_LOCAL} --query "
    SELECT n, dynamicType(v) AS type, v
    FROM file('${DATA_FILE}', Parquet)
    ORDER BY n
"

echo '--- paths ---'
${CLICKHOUSE_LOCAL} --query "
    SELECT n, JSONAllPathsWithTypes(dynamicElement(v, 'JSON')) AS paths
    FROM file('${DATA_FILE}', Parquet)
    WHERE dynamicType(v) = 'JSON'
    ORDER BY n
"
