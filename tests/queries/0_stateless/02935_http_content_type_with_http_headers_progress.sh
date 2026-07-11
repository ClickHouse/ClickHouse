#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

THREADS=$(nproc)

for format in TSV TabSeparatedWithNamesAndTypes CSV CSVWithNames Null Native RowBinary JSONStrings JSON JSONEachRow Values Vertical
do
  echo $format
  url="${CLICKHOUSE_URL}/?http_headers_progress_interval_ms=1&send_progress_in_http_headers=true&query=select+sleepEachRow(0.01)from+numbers(10)+FORMAT+${format}"
  (seq 1 ${THREADS} | xargs --max-args=1 -P"${THREADS}" -Ixxx curl -Ss -v -o /dev/null ${url} 2>&1 | grep -P -o " Content-Type:.*$") | strings | sort -u
done

