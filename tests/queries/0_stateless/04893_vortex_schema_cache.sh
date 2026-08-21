#!/usr/bin/env bash
# Tags: no-fasttest, no-msan


CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every setting that shapes an inferred type has to be part of the schema cache key, or a schema
# cached under one value gets handed out for another.

DATA_FILE=$CLICKHOUSE_TMP/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex

$CLICKHOUSE_LOCAL -q "
    SELECT
        number AS n,
        toString(number) AS s,
        if(number = 1, NULL, (number, toString(number))) AS t
    FROM numbers(3)
    INTO OUTFILE '$DATA_FILE' TRUNCATE FORMAT Vortex
"

$CLICKHOUSE_LOCAL -m -q "
    DESC file('$DATA_FILE', 'Vortex') SETTINGS schema_inference_make_columns_nullable = 1;
    DESC file('$DATA_FILE', 'Vortex') SETTINGS schema_inference_make_columns_nullable = 0;
    DESC file('$DATA_FILE', 'Vortex') SETTINGS schema_inference_make_columns_nullable = 1;
    DESC file('$DATA_FILE', 'Vortex') SETTINGS schema_inference_make_columns_nullable = 0, allow_experimental_nullable_tuple_type = 1;
    DESC file('$DATA_FILE', 'Vortex') SETTINGS schema_inference_make_columns_nullable = 0, allow_experimental_nullable_tuple_type = 0 FORMAT Null;
    DESC file('$DATA_FILE', 'Vortex') SETTINGS schema_inference_make_columns_nullable = 1, schema_inference_make_json_columns_nullable = 0 FORMAT Null;
    DESC file('$DATA_FILE', 'Vortex') SETTINGS schema_inference_make_columns_nullable = 1, schema_inference_make_json_columns_nullable = 1 FORMAT Null;
    SELECT count() FROM system.schema_inference_cache
    WHERE format = 'Vortex' AND additional_format_info LIKE '%schema_inference_make_columns_nullable%';
"

rm -f "$DATA_FILE"
