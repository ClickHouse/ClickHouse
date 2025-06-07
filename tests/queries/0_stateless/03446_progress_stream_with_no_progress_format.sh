#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CURL -sS -v -H "Accept: text/event-stream" "${CLICKHOUSE_URL}&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0" -d "SELECT sleep(0.5), number FROM numbers(1000) LIMIT 10 FORMAT JSONEachRowEventStream" 2>/dev/null | head -n38 | sed -r -e 's/,?"elapsed_ns":"[0-9]+"//'
