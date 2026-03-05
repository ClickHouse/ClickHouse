#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/"

# with progress
${CLICKHOUSE_CURL} -vsS "${URL}?send_progress_in_http_headers=1&enable_http_compression=1&wait_end_of_query=0" -o /dev/null \
  -H 'Accept-Encoding: zstd' --compressed --data-binary @- <<< "select distinct sleep(.1),name from generateRandom('name String',1,1000,2) limit 100009 format TSV" 2>&1 \
  | perl -lnE 'print if /Content-Encoding/';
# no progress
${CLICKHOUSE_CURL} -vsS "${URL}?send_progress_in_http_headers=0&enable_http_compression=1&wait_end_of_query=0" -o /dev/null \
  -H 'Accept-Encoding: zstd' --compressed --data-binary @- <<< "select distinct sleep(.1),name from generateRandom('name String',1,1000,2) limit 100009 format TSV" 2>&1 \
  | perl -lnE 'print if /Content-Encoding/';
