#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo '{"x" : 42}' > $CLICKHOUSE_TEST_UNIQUE_NAME.json
$CLICKHOUSE_LOCAL -m -q "
DESC file('$CLICKHOUSE_TEST_UNIQUE_NAME.json') SETTINGS input_format_max_bytes_to_read_for_schema_inference=1000; 
SELECT additional_format_info from system.schema_inference_cache"

rm $CLICKHOUSE_TEST_UNIQUE_NAME.json

