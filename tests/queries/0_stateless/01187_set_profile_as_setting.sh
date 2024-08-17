#!/usr/bin/env bash
# Tags: no-random-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# reset --log_comment, because the test has to use the readonly mode
CLICKHOUSE_LOG_COMMENT=
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "select value, changed from system.settings where name='readonly';"
$CLICKHOUSE_CLIENT -m -q "set profile='default'; select value, changed from system.settings where name='readonly';"
$CLICKHOUSE_CLIENT -m -q "set profile='readonly'; select value, changed from system.settings where name='readonly';" 2>&1| grep -Fa "Cannot modify 'send_logs_level' setting in readonly mode" > /dev/null && echo "OK"
CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=fatal/g')
$CLICKHOUSE_CLIENT -m -q "set profile='readonly'; select value, changed from system.settings where name='readonly';"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=select+value,changed+from+system.settings+where+name='readonly'"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&profile=default&query=select+value,changed+from+system.settings+where+name='readonly'"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&profile=readonly&query=select+value,changed+from+system.settings+where+name='readonly'" 2>&1| grep -Fa "Cannot modify 'readonly' setting in readonly mode" > /dev/null && echo "OK"
echo "select value, changed from system.settings where name='readonly';" | ${CLICKHOUSE_CURL} -sSg "${CLICKHOUSE_URL}&profile=readonly" -d @-
