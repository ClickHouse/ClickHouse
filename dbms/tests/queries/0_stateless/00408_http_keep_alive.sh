#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CURL} -vsS ${CLICKHOUSE_URL} --data-binary @- <<< "SELECT 1" 2>&1 | perl -lnE 'print if /Keep-Alive/';
${CLICKHOUSE_CURL} -vsS ${CLICKHOUSE_URL} --data-binary @- <<< " error here " 2>&1 | perl -lnE 'print if /Keep-Alive/';
${CLICKHOUSE_CURL} -vsS ${CLICKHOUSE_URL}ping  2>&1 | perl -lnE 'print if /Keep-Alive/';

# no keep-alive:
${CLICKHOUSE_CURL} -vsS ${CLICKHOUSE_URL}404/not/found/ 2>&1 | perl -lnE 'print if /Keep-Alive/';
