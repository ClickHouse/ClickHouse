#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

(seq 1 200| xargs -n1 -P0 -Ixxx curl -v "${CLICKHOUSE_URL}/?http_headers_progress_interval_ms=1&send_progress_in_http_headers=true&query=select+sleepEachRow(0.01)from+numbers(10)+FORMAT+CSV" 2>&1|grep -Eo " Content-Type:.*$")|sort -u
(seq 1 200| xargs -n1 -P0 -Ixxx curl -v "${CLICKHOUSE_URL}/?http_headers_progress_interval_ms=1&send_progress_in_http_headers=true&query=select+sleepEachRow(0.01)from+numbers(10)+FORMAT+JSON" 2>&1|grep -Eo " Content-Type:.*$")|sort -u
(seq 1 200| xargs -n1 -P0 -Ixxx curl -v "${CLICKHOUSE_URL}/?http_headers_progress_interval_ms=1&send_progress_in_http_headers=true&query=select+sleepEachRow(0.01)from+numbers(10)+FORMAT+TSV" 2>&1|grep -Eo " Content-Type:.*$")|sort -u

