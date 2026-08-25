#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&input_format_with_names_use_headers=1" -d 'SELECT 1' 2>&1 | grep -q "Code: 115.*Maybe you meant \['input_format_with_names_use_header','input_format_with_types_use_header'\]. (UNKNOWN_SETTING)" && echo 'OK' || echo 'FAIL' ||:
