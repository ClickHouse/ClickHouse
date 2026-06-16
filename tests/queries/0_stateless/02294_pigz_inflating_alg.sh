#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Round-trip a dataset through the parallel pigz deflater and inflater.
FILE="${CLICKHOUSE_TMP}/02294_pigz_inflating.csv.gz"
rm -f "$FILE"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM numbers(10000000) INTO OUTFILE '${FILE}' COMPRESSION 'pigz' FORMAT CSV"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test02294"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test02294 (x UInt64) ENGINE = Memory"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test02294 (x) FROM INFILE '${FILE}' COMPRESSION 'pigz' FORMAT CSV"
${CLICKHOUSE_CLIENT} --query "SELECT count(x) FROM test02294"
${CLICKHOUSE_CLIENT} --query "SELECT count(DISTINCT x) FROM test02294"

${CLICKHOUSE_CLIENT} --query "INSERT INTO test02294 (x) FROM INFILE '${FILE}' COMPRESSION 'pigz' FORMAT CSV"
${CLICKHOUSE_CLIENT} --query "SELECT count(x) FROM test02294"
${CLICKHOUSE_CLIENT} --query "SELECT count(DISTINCT x) FROM test02294"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test02294"
rm -f "$FILE"
