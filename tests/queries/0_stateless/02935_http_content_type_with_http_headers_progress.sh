#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for frmt in TSV TabSeparatedWithNamesAndTypes CSV CSVWithNames Null Native RowBinary JSONStrings JSON JSONEachRow Values Vertical
do
  echo $frmt
  url="${CLICKHOUSE_URL}/?http_headers_progress_interval_ms=1&send_progress_in_http_headers=true&query=select+sleepEachRow(0.01)from+numbers(10)+FORMAT+${frmt}"
  (seq 1 200| xargs -n1 -P0 -Ixxx curl -Ss -v -o /dev/null ${url} 2>&1|grep -Eo " Content-Type:.*$")|strings|sort -u
done

echo "JSONEachRow output_format_json_content_type_header"
url="${CLICKHOUSE_URL}&output_format_json_content_type_header=custom-type&query=select+1+FORMAT+JSONEachRow"
curl -Ss -v $url 2>&1|grep -Eo " Content-Type:.*$"