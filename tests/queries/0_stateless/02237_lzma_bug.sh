#!/usr/bin/env bash
# Tags: no-fasttest


CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS file"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE file (x UInt64) ENGINE = File(TSV, '${CLICKHOUSE_DATABASE}/data.tsv.xz')"
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE file"
${CLICKHOUSE_CLIENT} --query "INSERT INTO file SELECT * FROM numbers(1000000)"
${CLICKHOUSE_CLIENT} --max_read_buffer_size=8 --query "SELECT count(), max(x) FROM file"
${CLICKHOUSE_CLIENT} --query "DROP TABLE file"

