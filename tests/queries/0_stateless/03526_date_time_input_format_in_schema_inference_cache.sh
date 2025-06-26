#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo '{"x" : "2025 May 1"}' > $CLICKHOUSE_TEST_UNIQUE_NAME.json
$CLICKHOUSE_LOCAL -m -q "
DESC file('$CLICKHOUSE_TEST_UNIQUE_NAME.json') SETTINGS date_time_input_format='basic';
DESC file('$CLICKHOUSE_TEST_UNIQUE_NAME.json') SETTINGS date_time_input_format='best_effort';
SELECT additional_format_info from system.schema_inference_cache"

rm $CLICKHOUSE_TEST_UNIQUE_NAME.json

