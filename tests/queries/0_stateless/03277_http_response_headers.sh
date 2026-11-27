#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "We can add a new header:"
${CLICKHOUSE_CURL} -sS --globoff -v "http://localhost:8123/?http_response_headers={'My-New-Header':'Hello,+world.'}" -d "SELECT 1" 2>&1 | grep -i 'My-New'

echo "It works even with the settings clause:"
${CLICKHOUSE_CURL} -sS --globoff -v "http://localhost:8123/" -d "SELECT 1 SETTINGS http_response_headers = \$\${'My-New-Header':'Hello, world.'}\$\$" 2>&1 | grep -i 'My-New'

echo "Check the default header value:"
${CLICKHOUSE_CURL} -sS --globoff -v "http://localhost:8123/" -d "SELECT 1" 2>&1 | grep -i 'Content-Type'

echo "Check that we can override it:"
${CLICKHOUSE_CURL} -sS --globoff -v "http://localhost:8123/?http_response_headers={'Content-Type':'image/png'}" -d "SELECT 1" 2>&1 | grep -i 'Content-Type'

echo "It does not allow bad characters:"
${CLICKHOUSE_CURL} -sS --globoff -v "http://localhost:8123/" -d "SELECT 1 SETTINGS http_response_headers = \$\${'My-New-Header':'Hello,\n\nworld.'}\$\$" 2>&1 | grep -o -F 'BAD_ARGUMENTS'
${CLICKHOUSE_CURL} -sS --globoff -v "http://localhost:8123/" -d "SELECT 1 SETTINGS http_response_headers = \$\${'My\rNew-Header':'Hello, world.'}\$\$" 2>&1 | grep -o -F 'BAD_ARGUMENTS'

echo "It does not let duplicate entries:"
${CLICKHOUSE_CURL} -sS --globoff -v "http://localhost:8123/" -d "SELECT 1 SETTINGS http_response_headers = \$\${'a':'b','a':'c'}\$\$" 2>&1 | grep -o -F 'BAD_ARGUMENTS'
