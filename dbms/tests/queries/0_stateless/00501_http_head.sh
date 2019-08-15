#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

( ${CLICKHOUSE_CURL} -s --head "${CLICKHOUSE_URL}?query=SELECT%201";
  ${CLICKHOUSE_CURL} -s --head "${CLICKHOUSE_URL}?query=select+*+from+system.numbers+limit+1000000" ) | grep -v "Date:" | grep -v "X-ClickHouse-Server-Display-Name:" | grep -v "X-ClickHouse-Query-Id:"

if [[ `${CLICKHOUSE_CURL} -sS -X POST -I "${CLICKHOUSE_URL}?query=SELECT+1" | grep -c '411 Length Required'` -ne 1 ]]; then
    echo FAIL
fi
