#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A query broken off by `timeout_overflow_mode = 'break'` returns its partial result as a success, so the
# output format still has to write its epilogue. Otherwise the response is a truncated document - or, as
# here, an empty one instead of a `JSON` object or an `XML` document - and the `statistics` section with
# `rows_read` is missing altogether.
#
# The scan of `numbers(10000000000)` cannot finish within `max_execution_time`, so the query is always
# broken off, and it always returns no rows: `materialize` keeps the filter from being evaluated at
# analysis time, so every row is read and rejected.

QUERY="SELECT number FROM numbers(10000000000) WHERE materialize(0) = 1"
URL="${CLICKHOUSE_URL}&max_execution_time=1&timeout_overflow_mode=break&max_threads=2"

${CLICKHOUSE_CURL} -sS "$URL" -d "$QUERY FORMAT JSON" > "${CLICKHOUSE_TMP}/05077.json"
${CLICKHOUSE_LOCAL} --query "
    SELECT isValidJSON(response) AS valid_json, JSONExtractUInt(response, 'rows') AS rows, JSONHas(response, 'statistics', 'rows_read') AS has_statistics
    FROM (SELECT (SELECT * FROM file('${CLICKHOUSE_TMP}/05077.json', RawBLOB)) AS response)
    FORMAT TSVWithNames"

${CLICKHOUSE_CURL} -sS "$URL" -d "$QUERY FORMAT XML" | grep -o -E '<rows>[0-9]+</rows>|<rows_read>|</result>'
