#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_URL="$CLICKHOUSE_URL&http_write_exception_in_output_format=1"

${CLICKHOUSE_CURL} -sS "$CH_URL" -d "SELECT repeat('1 ', 1000000) from system.one format JSONEachRow settings output_format_json_validate_utf8=1" | wc -w

