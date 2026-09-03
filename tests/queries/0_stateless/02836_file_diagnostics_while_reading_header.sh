#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FILENAME="${CLICKHOUSE_TMP}/test.csv"

printf 'Bad,Header\n123\n' > "${FILENAME}"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${CLICKHOUSE_TMP}/t*e*s*t.csv')" 2>&1 | grep -o -P 'in file/uri|test\.csv'
rm "${FILENAME}"
