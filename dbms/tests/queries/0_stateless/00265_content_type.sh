#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CURL} -vsS ${CLICKHOUSE_URL}?default_format=JSONCompact --data-binary @- <<< "SELECT 1" 2>&1 | grep '< Content-Type';
${CLICKHOUSE_CURL} -vsS ${CLICKHOUSE_URL} --data-binary @- <<< "SELECT 1 FORMAT JSON"         2>&1 | grep '< Content-Type';
${CLICKHOUSE_CURL} -vsS ${CLICKHOUSE_URL} --data-binary @- <<< "SELECT 1"                     2>&1 | grep '< Content-Type';
${CLICKHOUSE_CURL} -vsS ${CLICKHOUSE_URL} --data-binary @- <<< "SELECT 1 FORMAT TabSeparated" 2>&1 | grep '< Content-Type';
${CLICKHOUSE_CURL} -vsS ${CLICKHOUSE_URL} --data-binary @- <<< "SELECT 1 FORMAT Vertical"     2>&1 | grep '< Content-Type';
${CLICKHOUSE_CURL} -vsS ${CLICKHOUSE_URL} --data-binary @- <<< "SELECT 1 FORMAT Native"       2>&1 | grep '< Content-Type';
${CLICKHOUSE_CURL} -vsS ${CLICKHOUSE_URL} --data-binary @- <<< "SELECT 1 FORMAT RowBinary"    2>&1 | grep '< Content-Type';
