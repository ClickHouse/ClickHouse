#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When formatting fails mid-stream over HTTP with http_write_exception_in_output_format=1, the GeoJSON
# output must remain valid JSON: the exception is appended as an object element of the `features` array,
# not as a bare "exception" member. The exception text is version-dependent, so check JSON validity
# rather than comparing the message.
CH_URL="${CLICKHOUSE_URL}&http_write_exception_in_output_format=1"

for parallel in 0 1
do
    echo "parallel=$parallel"
    # A single row that fails immediately because of a non-finite coordinate.
    ${CLICKHOUSE_CURL} -sS "$CH_URL" -d "SELECT (nan, 2.0)::Point AS geometry FORMAT GeoJSON SETTINGS output_format_parallel_formatting=$parallel" \
        | ${CLICKHOUSE_LOCAL} --input-format=JSONAsString -q "SELECT isValidJSON(json) FROM table"
    # Several rows are written successfully before one fails.
    ${CLICKHOUSE_CURL} -sS "$CH_URL" -d "SELECT (number = 3 ? nan : number + 0.0, 2.0)::Point AS geometry FROM numbers(10) FORMAT GeoJSON SETTINGS output_format_parallel_formatting=$parallel, max_block_size=1" \
        | ${CLICKHOUSE_LOCAL} --input-format=JSONAsString -q "SELECT isValidJSON(json) FROM table"
done
