#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: Uses failpoints, which break concurrent queries.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A secondary failure while updating the output format on the exception path (here a non-DB
# `std::bad_function_call` injected by the failpoint) must not replace the original query
# exception: the framed response still ends with an `exception` packet carrying the original
# error.

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"

${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT execute_query_calling_empty_set_result_func_on_exception"

echo '--- the original exception is framed despite a non-DB secondary failure on the exception path'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT 1 FROM table_04629_does_not_exist FORMAT JSON" \
    | grep -o -m1 '"packet":"exception"'
${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString" \
    -d "SELECT 1 FROM table_04629_does_not_exist FORMAT JSON" \
    | grep -o -m1 'UNKNOWN_TABLE'

${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT execute_query_calling_empty_set_result_func_on_exception"
