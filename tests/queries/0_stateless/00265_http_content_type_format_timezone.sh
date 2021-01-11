#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_TIMEZONE_ESCAPED=$($CLICKHOUSE_CLIENT --query="SELECT timezone()" | sed 's/[]\/$*.^+:()[]/\\&/g')

${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&default_format=JSONCompact" --data-binary @- <<< "SELECT 1" 2>&1 | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" --data-binary @- <<< "SELECT 1 FORMAT JSON"         2>&1 | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" --data-binary @- <<< "SELECT 1"                     2>&1 | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" --data-binary @- <<< "SELECT 1 FORMAT TabSeparated" 2>&1 | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" --data-binary @- <<< "SELECT 1 FORMAT Vertical"     2>&1 | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" --data-binary @- <<< "SELECT 1 FORMAT Native"       2>&1 | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" --data-binary @- <<< "SELECT 1 FORMAT RowBinary"    2>&1 | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;
