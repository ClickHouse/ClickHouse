#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `http_allow_filters_as_unrecognized_url_parameters` is on by default, which turns an unrecognized
# URL parameter into a filter instead of rejecting it as an unknown setting. Switch it off so the
# misspelt setting name below reaches the settings pipeline and produces the hint.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&http_allow_filters_as_unrecognized_url_parameters=0&input_format_with_names_use_headers=1" -d 'SELECT 1' 2>&1 | grep -q "Code: 115.*Maybe you meant \['input_format_with_names_use_header','input_format_with_types_use_header'\]. (UNKNOWN_SETTING)" && echo 'OK' || echo 'FAIL' ||:
