#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

# POST permits everything.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=SELECT+name,value,changed+FROM+system.settings+WHERE+name+IN+('readonly','max_rows_to_read')&max_rows_to_read=10000&default_format=PrettySpaceNoEscapes" -d' '

# GET implies readonly = 2.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=SELECT+name,value,changed+FROM+system.settings+WHERE+name+IN+('readonly','max_rows_to_read')&max_rows_to_read=10000&default_format=PrettySpaceNoEscapes"

# It is possible to simultaneously set more strict variant of readonly and specify some other settings.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=SELECT+name,value,changed+FROM+system.settings+WHERE+name+IN+('readonly','max_rows_to_read')&readonly=1&max_rows_to_read=10000&default_format=PrettySpaceNoEscapes" -d' '
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=SELECT+name,value,changed+FROM+system.settings+WHERE+name+IN+('readonly','max_rows_to_read')&readonly=2&max_rows_to_read=10000&default_format=PrettySpaceNoEscapes" -d' '

${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}?query=DROP+TABLE+IF+EXISTS+nonexistent" 2>&1 | grep -q '500 Internal Server Error' && echo 'Ok' || echo 'Fail'
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}?readonly=0&query=DROP+TABLE+IF+EXISTS+nonexistent" 2>&1 | grep -q '500 Internal Server Error' && echo 'Ok' || echo 'Fail'

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?query=DROP+TABLE+IF+EXISTS+nonexistent" -d ' ' | wc -l
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}?readonly=0&query=DROP+TABLE+IF+EXISTS+nonexistent" -d ' ' | wc -l

${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}?readonly=1&query=DROP+TABLE+IF+EXISTS+nonexistent" -d ' ' 2>&1 | grep -q '500 Internal Server Error' && echo 'Ok' || echo 'Fail'
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}?readonly=2&query=DROP+TABLE+IF+EXISTS+nonexistent" -d ' ' 2>&1 | grep -q '500 Internal Server Error' && echo 'Ok' || echo 'Fail'
