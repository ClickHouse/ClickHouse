#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# POST permits everything.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+name,value,changed+FROM+system.settings+WHERE+name+IN+('readonly','max_rows_to_read')&max_rows_to_read=10000&default_format=PrettySpaceNoEscapes" -d' '

# GET implies readonly = 2.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+name,value,changed+FROM+system.settings+WHERE+name+IN+('readonly','max_rows_to_read')&max_rows_to_read=10000&default_format=PrettySpaceNoEscapes"

# It is possible to simultaneously set more strict variant of readonly and specify some other settings.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+name,value,changed+FROM+system.settings+WHERE+name+IN+('readonly','max_rows_to_read')&readonly=1&max_rows_to_read=10000&default_format=PrettySpaceNoEscapes" -d' '
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=SELECT+name,value,changed+FROM+system.settings+WHERE+name+IN+('readonly','max_rows_to_read')&readonly=2&max_rows_to_read=10000&default_format=PrettySpaceNoEscapes" -d' '

${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&query=CREATE+TABLE+table_00305a(x+Int8)+ENGINE=Log" 2>&1 | grep -q '500 Internal Server Error' && echo 'Ok' || echo 'Fail'
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&readonly=0&query=CREATE+TABLE+table_00305a(x+Int8)+ENGINE=Log" 2>&1 | grep -q '500 Internal Server Error' && echo 'Ok' || echo 'Fail'

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=CREATE+TABLE+table_00305a(x+Int8)+ENGINE=Log" -d ' ' | wc -l
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&readonly=0&query=CREATE+TABLE+table_00305b(x+Int8)+ENGINE=Log" -d ' ' | wc -l

${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&readonly=1&query=CREATE+TABLE+table_00305c(x+Int8)+ENGINE=Log" -d ' ' 2>&1 | grep -q '500 Internal Server Error' && echo 'Ok' || echo 'Fail'
${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&readonly=2&query=CREATE+TABLE+table_00305c(x+Int8)+ENGINE=Log" -d ' ' 2>&1 | grep -q '500 Internal Server Error' && echo 'Ok' || echo 'Fail'

${CLICKHOUSE_CURL} -vsS "${CLICKHOUSE_URL}&query=DROP+TABLE+table_00305a" 2>&1 | grep -q '500 Internal Server Error' && echo 'Ok' || echo 'Fail'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=DROP+TABLE+table_00305a" -d ' ' | wc -l
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=DROP+TABLE+table_00305b" -d ' ' | wc -l
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=DROP+TABLE+IF+EXISTS+table_00305c" | wc -l
