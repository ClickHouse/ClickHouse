#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TMP_FILE="${CLICKHOUSE_TMP}/file.tsv"
echo '1' > "${TMP_FILE}"
${CLICKHOUSE_LOCAL} --query "SELECT count() from '${TMP_FILE}'"
${CLICKHOUSE_LOCAL} --query "select 2 as a, (SELECT count() from '${TMP_FILE}') as b"
rm "${TMP_FILE}"
