#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# the sed command here replaces the real number of left requests with a question mark, because it can vary and we don't really have control over it
( ${CLICKHOUSE_CURL} -s --head "${CLICKHOUSE_URL}&query=SELECT%201" | sed -r 's/(keep-alive: timeout=10, max=)[0-9]+/\1?/I';
  ${CLICKHOUSE_CURL} -s --head "${CLICKHOUSE_URL}&query=select+*+from+system.numbers+limit+1000000" ) | sed -r 's/(keep-alive: timeout=10, max=)[0-9]+/\1?/I' | grep -v "Date:" | grep -v "X-ClickHouse-Server-Display-Name:" | grep -v "X-ClickHouse-Query-Id:" | grep -v "X-ClickHouse-Format:" | grep -v "X-ClickHouse-Timezone:"

if [[ $(${CLICKHOUSE_CURL} -sS -X POST -I "${CLICKHOUSE_URL}&query=SELECT+1" | grep -c '411 Length Required') -ne 1 ]]; then
    echo FAIL
fi
