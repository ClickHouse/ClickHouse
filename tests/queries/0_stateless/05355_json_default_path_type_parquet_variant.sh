#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A Parquet variant object read as `JSON(DEFAULT PATH TYPE T)`: non-typed paths are converted to `T`
# both in dynamic paths and in shared data, the same as a `CAST` from `JSON`.
DATA_FILE=$CUR_DIR/data_parquet/05297_variant_json_shared_data.parquet

for T in 'Int64' 'String' 'LowCardinality(String)'; do
    TYPE="JSON(DEFAULT PATH TYPE $T, max_dynamic_paths = 2, k0001 UInt8)"
    ${CLICKHOUSE_LOCAL} --query "
        WITH (SELECT dynamicElement(v, 'JSON') FROM file('${DATA_FILE}', Parquet)) AS expected
        SELECT
            '$T',
            toString(v) = toString(expected::$TYPE),
            length(JSONDynamicPaths(v)),
            length(JSONSharedDataPaths(v)),
            v.k0001, toTypeName(v.k0001),
            v.k0500, toTypeName(v.k0500),
            v.k1099, toTypeName(v.k1099)
        FROM file('${DATA_FILE}', Parquet, 'n Int32, v $TYPE')
    "
done

# A value that cannot be converted to `T` is an error, like for typed paths.
DATA_FILE=$CUR_DIR/data_parquet/05296_variant_json_objects.parquet
${CLICKHOUSE_LOCAL} --query "
    SELECT * FROM file('${DATA_FILE}', Parquet, 'n Int32, v JSON(DEFAULT PATH TYPE Int64)')
" 2>&1 | grep -o -m1 'CANNOT_PARSE_TEXT'

# With `type_json_skip_invalid_typed_paths` such a value is skipped and the path is missing in the row,
# both when the path is a dynamic path and when it goes to shared data.
for max_dynamic_paths in 10 0; do
    ${CLICKHOUSE_LOCAL} --query "
        SELECT n, v, JSONDynamicPaths(v), JSONSharedDataPaths(v)
        FROM file('${DATA_FILE}', Parquet, 'n Int32, v JSON(DEFAULT PATH TYPE UInt8, max_dynamic_paths = $max_dynamic_paths)')
        WHERE n = 1
        SETTINGS type_json_skip_invalid_typed_paths = 1
    "
done

# The skipped value doesn't take a slot of `max_dynamic_paths`: `b.d.e` stays a dynamic path.
${CLICKHOUSE_LOCAL} --query "
    SELECT n, v, JSONDynamicPaths(v), JSONSharedDataPaths(v)
    FROM file('${DATA_FILE}', Parquet, 'n Int32, v JSON(DEFAULT PATH TYPE UInt8, max_dynamic_paths = 2)')
    WHERE n = 1
    SETTINGS type_json_skip_invalid_typed_paths = 1
"
