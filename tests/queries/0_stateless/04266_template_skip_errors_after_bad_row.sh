#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=${CLICKHOUSE_TEST_UNIQUE_NAME}.data
TEMPLATE_FILE=${CLICKHOUSE_TEST_UNIQUE_NAME}.template

trap 'rm -f "$DATA_FILE" "$TEMPLATE_FILE"' EXIT

{
    printf '%s\n' '1 Alice 100'
    printf '%s\n' '2 Bob -'
    printf '%s' '3 Carol 300'
} > "$DATA_FILE"

printf '%s' '${id:CSV} ${name:CSV} ${value:CSV}' > "$TEMPLATE_FILE"

$CLICKHOUSE_LOCAL --query "
    SELECT id, name, value
    FROM file('$DATA_FILE', Template, 'id UInt64, name String, value UInt64')
    ORDER BY id
    SETTINGS
        format_template_row='$TEMPLATE_FILE',
        input_format_allow_errors_ratio=0.9"
