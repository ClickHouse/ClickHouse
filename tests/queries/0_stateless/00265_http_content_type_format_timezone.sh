#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_TIMEZONE_ESCAPED=$($CLICKHOUSE_CLIENT --query="SELECT serverTimezone()" | sed 's/[]\/$*.^+:()[]/\\&/g')

# Remove randomized session_timezone from URL so that X-ClickHouse-Timezone header matches the server timezone
CLICKHOUSE_URL_WO_SESSION_TZ_1=$(echo "${CLICKHOUSE_URL}" | sed 's/\&session_timezone\=[A-Za-z0-9\/\%\_\-\+\-]*//g' | sed 's/\?session_timezone\=[A-Za-z0-9\/\%\_\-\+\-]*\&/\?/g')

tmpdir="$(mktemp -d "${CLICKHOUSE_TMP}/00265_http_content_type_format_timezone.XXXXXX")"
trap 'rm -rf "$tmpdir"' EXIT

# Every request below must answer with `X-ClickHouse-Timezone`, including the deliberately failing
# query, which still reports it. A missing header means the request never reached query execution,
# e.g. the server refused it before parsing; a non-zero `curl` exit means the request itself
# failed, e.g. the connection was reset, which counts even when the headers had already arrived.
# Report either on stderr: piping `curl`'s merged output into `grep` drops the diagnostic and
# leaves only missing header lines.
# Keep the response body in a file, it is binary for `Native` and `RowBinary`.
curl_headers() {
    # Truncate first: on an early transport failure curl never opens -o, so the previous request's
    # body would be reported as belonging to this one.
    : > "$tmpdir/body"
    : > "$tmpdir/diag"
    ${CLICKHOUSE_CURL} -vsS -o "$tmpdir/body" "$@" 2>"$tmpdir/diag"
    local rc=$?
    # Anchored: `Access-Control-Expose-Headers` lists this name on every response.
    if [ $rc -ne 0 ] || ! grep -qE '^< X-ClickHouse-Timezone:' "$tmpdir/diag"; then
        echo "HTTP request failed or did not reach query execution (curl exit $rc):" >&2
        cat "$tmpdir/diag" "$tmpdir/body" >&2
        return 1
    fi
    cat "$tmpdir/diag"
}

curl_headers "${CLICKHOUSE_URL_WO_SESSION_TZ_1}&default_format=JSONCompact" --data-binary @- <<< "SELECT 1" | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;
curl_headers "${CLICKHOUSE_URL_WO_SESSION_TZ_1}" --data-binary @- <<< "SELECT 1 FORMAT JSON"         | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;
curl_headers "${CLICKHOUSE_URL_WO_SESSION_TZ_1}" --data-binary @- <<< "SELECT 1"                     | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;
curl_headers "${CLICKHOUSE_URL_WO_SESSION_TZ_1}" --data-binary @- <<< "SELECT 1 FORMAT TabSeparated" | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;
curl_headers "${CLICKHOUSE_URL_WO_SESSION_TZ_1}" --data-binary @- <<< "SELECT 1 FORMAT Vertical"     | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;
curl_headers "${CLICKHOUSE_URL_WO_SESSION_TZ_1}" --data-binary @- <<< "SELECT 1 FORMAT Native"       | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;
curl_headers "${CLICKHOUSE_URL_WO_SESSION_TZ_1}" --data-binary @- <<< "SELECT 1 FORMAT RowBinary"    | grep -e '< Content-Type' -e '< X-ClickHouse-Format' -e '< X-ClickHouse-Timezone' | sed "s|$CLICKHOUSE_TIMEZONE_ESCAPED|CLICKHOUSE_TIMEZONE|" | sed 's/\r$//' | sort;

curl_headers "${CLICKHOUSE_URL}" --data-binary @- <<< "SELECT timezone() SETTINGS session_timezone='Europe/Berlin'" | grep '< X-ClickHouse-Timezone' | grep -v 'GET' | tr -d '\r';
curl_headers "${CLICKHOUSE_URL}" --data-binary @- <<< "SELECT timezone() SETTINGS session_timezone='Africa/Cairo'"  | grep '< X-ClickHouse-Timezone' | grep -v 'GET' | tr -d '\r';

# Not pretty but working way of removing randomized session_timezone for this part of test
CLICKHOUSE_URL_WO_SESSION_TZ=$(echo "${CLICKHOUSE_URL}" |sed 's/\&session_timezone\=[A-Za-z0-9\/\%\_\-\+\-]*//g' | sed 's/\?session_timezone\=[A-Za-z0-9\/\%\_\-\+\-]*\&/\?/g');

curl_headers "${CLICKHOUSE_URL_WO_SESSION_TZ}&session_timezone=Europe/Berlin&query=SELECT+timezone()" | grep '< X-ClickHouse-Timezone' | grep -v 'GET' | tr -d '\r';
curl_headers "${CLICKHOUSE_URL_WO_SESSION_TZ}&session_timezone=America/Denver&query=SELECT+timezone()" | grep '< X-ClickHouse-Timezone' | grep -v 'GET' | tr -d '\r';
# check that proper X-ClickHouse-Timezone returned on query fail
curl_headers "${CLICKHOUSE_URL_WO_SESSION_TZ}&session_timezone=UTC&query=SELECT+intDiv(1,+(3600-timeZoneOffset('2024-05-06+12:00:00'::DateTime)))+SETTINGS+session_timezone+=+'Europe/Lisbon'" | grep '< X-ClickHouse-Timezone' | grep -v 'GET' | tr -d '\r';
# main query's session_timezone shall be set in header
curl_headers "${CLICKHOUSE_URL_WO_SESSION_TZ}&session_timezone=America/New_York&query=SELECT+1,(SELECT+1+SETTINGS+session_timezone='UTC')+SETTINGS+session_timezone='Europe/Lisbon'" | grep '< X-ClickHouse-Timezone' | grep -v 'GET' | tr -d '\r';
