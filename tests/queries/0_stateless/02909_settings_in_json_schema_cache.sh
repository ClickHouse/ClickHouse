#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo '{"x" : 42}' > $CLICKHOUSE_TEST_UNIQUE_NAME.json
$CLICKHOUSE_LOCAL -m -q "
DESC file('$CLICKHOUSE_TEST_UNIQUE_NAME.json') SETTINGS schema_inference_make_columns_nullable=1;
DESC file('$CLICKHOUSE_TEST_UNIQUE_NAME.json') SETTINGS schema_inference_make_columns_nullable=0;
SELECT count() from system.schema_inference_cache where format = 'JSON' and additional_format_info like '%schema_inference_make_columns_nullable%';"

rm $CLICKHOUSE_TEST_UNIQUE_NAME.json

