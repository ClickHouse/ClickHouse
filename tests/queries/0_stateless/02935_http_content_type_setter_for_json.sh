#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo "JSONEachRow output_format_json_content_type_header"
url="${CLICKHOUSE_URL}&output_format_json_content_type_header=custom-type&query=select+1+FORMAT+JSONEachRow"
(seq 1 200| xargs -n1 -P0 -Ixxx curl -Ss -v -o /dev/null ${url} 2>&1|grep -Eo " Content-Type:.*$")|strings|sort -u