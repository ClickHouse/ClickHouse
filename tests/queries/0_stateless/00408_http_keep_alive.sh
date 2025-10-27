#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/"

# the sed command here replaces the real number of left requests with a question mark, because it can vary and we don't really have control over it
${CLICKHOUSE_CURL} -vsS "${URL}" --data-binary @- <<< "SELECT 1" 2>&1 | sed -r 's/(keep-alive: timeout=10, max=)[0-9]+/\1?/I' | grep -i 'keep-alive';
${CLICKHOUSE_CURL} -vsS "${URL}" --data-binary @- <<< " error here " 2>&1 | sed -r 's/(keep-alive: timeout=10, max=)[0-9]+/\1?/I' | grep -i 'keep-alive';
${CLICKHOUSE_CURL} -vsS "${URL}"ping  2>&1 | perl -lnE 'print if /Keep-Alive/' | sed -r 's/(keep-alive: timeout=10, max=)[0-9]+/\1?/I' | grep -i 'keep-alive';

# no keep-alive:
${CLICKHOUSE_CURL} -vsS "${URL}"404/not/found/ 2>&1 | perl -lnE 'print if /Keep-Alive/';
