#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

# The settings that shape the inferred types must be a part of the schema cache key, otherwise
# a cached schema is reused for a different value of the setting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TMP/test_$CLICKHOUSE_TEST_UNIQUE_NAME.vortex

$CLICKHOUSE_LOCAL -q "
    SELECT number AS n, toString(number) AS s FROM numbers(3)
    INTO OUTFILE '$DATA_FILE' TRUNCATE FORMAT Vortex
"

$CLICKHOUSE_LOCAL -m -q "
    DESC file('$DATA_FILE', 'Vortex') SETTINGS schema_inference_make_columns_nullable = 1;
    DESC file('$DATA_FILE', 'Vortex') SETTINGS schema_inference_make_columns_nullable = 0;
    DESC file('$DATA_FILE', 'Vortex') SETTINGS schema_inference_make_columns_nullable = 1;
    DESC file('$DATA_FILE', 'Vortex') SETTINGS schema_inference_make_json_columns_nullable = 1;
    DESC file('$DATA_FILE', 'Vortex') SETTINGS schema_inference_make_json_columns_nullable = 0;
    SELECT count() FROM system.schema_inference_cache
    WHERE format = 'Vortex' AND additional_format_info LIKE '%schema_inference_make_columns_nullable%';
"

rm -f "$DATA_FILE"
