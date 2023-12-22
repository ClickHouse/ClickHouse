#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

if ! command -v gzip &> /dev/null; then echo "gzip not found" 1>&2; exit 1; fi

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS file"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE file (x UInt64) ENGINE = File(TSV, '${CLICKHOUSE_DATABASE}/test.tsv')"
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE file"
${CLICKHOUSE_CLIENT} --query "INSERT INTO file SELECT * FROM numbers(1000000)"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file ORDER BY x INTO OUTFILE '${CLICKHOUSE_TMP}/table.tsv'"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file ORDER BY x INTO OUTFILE '${CLICKHOUSE_TMP}/compressed_table.tsv.gz' COMPRESSION 'pigz'"

gzip -d ${CLICKHOUSE_TMP}/compressed_table.tsv.gz

diff ${CLICKHOUSE_TMP}/table.tsv ${CLICKHOUSE_TMP}/compressed_table.tsv
