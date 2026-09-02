#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

[ -e "${CLICKHOUSE_TMP}"/test_append_to_output_file ] && rm "${CLICKHOUSE_TMP}"/test_append_to_output_file

${CLICKHOUSE_CLIENT} --query "SELECT * FROM (SELECT 'Hello, World! From client.') INTO OUTFILE '${CLICKHOUSE_TMP}/test_append_to_output_file'"
${CLICKHOUSE_LOCAL} --query "SELECT * FROM (SELECT 'Hello, World! From local.') INTO OUTFILE '${CLICKHOUSE_TMP}/test_append_to_output_file' APPEND"
cat ${CLICKHOUSE_TMP}/test_append_to_output_file

rm -f "${CLICKHOUSE_TMP}/test_append_to_output_file"
