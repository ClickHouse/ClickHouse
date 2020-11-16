#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/"

${CLICKHOUSE_CURL} -vsS "${URL}" --data-binary @- <<< "SELECT 1" 2>&1 | perl -lnE 'print if /Keep-Alive/';
${CLICKHOUSE_CURL} -vsS "${URL}" --data-binary @- <<< " error here " 2>&1 | perl -lnE 'print if /Keep-Alive/';
${CLICKHOUSE_CURL} -vsS "${URL}"ping  2>&1 | perl -lnE 'print if /Keep-Alive/';

# no keep-alive:
${CLICKHOUSE_CURL} -vsS "${URL}"404/not/found/ 2>&1 | perl -lnE 'print if /Keep-Alive/';
