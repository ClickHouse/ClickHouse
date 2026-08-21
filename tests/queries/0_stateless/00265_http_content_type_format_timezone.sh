#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_TIMEZONE_ESCAPED=$($CLICKHOUSE_CLIENT --query="SELECT timezone()" | sed 's/[]\/$*.^+:()[]/\\&/g')

${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&default_format=JSONCompact" --data-binary @- <<< "SELECT 1" 2>&1 | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" --data-binary @- <<< "SELECT 1 FORMAT JSON"         2>&1 | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" --data-binary @- <<< "SELECT 1"                     2>&1 | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" --data-binary @- <<< "SELECT 1 FORMAT TabSeparated" 2>&1 | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" --data-binary @- <<< "SELECT 1 FORMAT Vertical"     2>&1 | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" --data-binary @- <<< "SELECT 1 FORMAT Native"       2>&1 | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" --data-binary @- <<< "SELECT 1 FORMAT RowBinary"    2>&1 | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;

${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" --data-binary @- <<< "SELECT timezone() SETTINGS session_timezone='Europe/Berlin'" 2>&1 | grep '< X-ClickHouse-Timezone' | grep -v 'GET' | tr -d '\r';
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}" --data-binary @- <<< "SELECT timezone() SETTINGS session_timezone='Africa/Cairo'"  2>&1 | grep '< X-ClickHouse-Timezone' | grep -v 'GET' | tr -d '\r';

# Not pretty but working way of removing randomized session_timezone for this part of test
CLICKHOUSE_URL_WO_SESSION_TZ=$(echo "${CLICKHOUSE_URL}" |sed 's/\&session_timezone\=[A-Za-z0-9\/\%\_\-\+\-]*//g' | sed 's/\?session_timezone\=[A-Za-z0-9\/\%\_\-\+\-]*\&/\?/g');

${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL_WO_SESSION_TZ}&session_timezone=Europe/Berlin&query=SELECT+timezone()" 2>&1 | grep '< X-ClickHouse-Timezone' | grep -v 'GET' | tr -d '\r';
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL_WO_SESSION_TZ}&session_timezone=America/Denver&query=SELECT+timezone()" 2>&1 | grep '< X-ClickHouse-Timezone' | grep -v 'GET' | tr -d '\r';
# check that proper X-ClickHouse-Timezone returned on query fail
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL_WO_SESSION_TZ}&session_timezone=UTC&query=SELECT+intDiv(1,+(3600-timeZoneOffset('2024-05-06+12:00:00'::DateTime)))+SETTINGS+session_timezone+=+'Europe/Lisbon'" 2>&1 | grep '< X-ClickHouse-Timezone' | grep -v 'GET' | tr -d '\r';
# main query's session_timezone shall be set in header
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL_WO_SESSION_TZ}&session_timezone=America/New_York&query=SELECT+1,(SELECT+1+SETTINGS+session_timezone='UTC')+SETTINGS+session_timezone='Europe/Lisbon'" 2>&1 | grep '< X-ClickHouse-Timezone' | grep -v 'GET' | tr -d '\r';
