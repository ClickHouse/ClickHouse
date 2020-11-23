#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS data"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE data (key Int) Engine=Memory()"
${CLICKHOUSE_CLIENT} --input_format_parallel_parsing=0 -q "INSERT INTO data SELECT key FROM input('key Int') FORMAT TSV" <<<10
# with '\n...' after the query clickhouse-client prefer data from the query over data from stdin, and produce very tricky message:
#   Code: 27. DB::Exception: Cannot parse input: expected '\n' before: ' ': (at row 1)
# well for TSV it is ok, but for RowBinary:
#   Code: 33. DB::Exception: Cannot read all data. Bytes read: 1. Bytes expected: 4.
# so check that the exception message contain the data source.
${CLICKHOUSE_CLIENT} --input_format_parallel_parsing=0 -q "INSERT INTO data FORMAT TSV
 " <<<2 |& grep -F -c 'data for INSERT was parsed from query'
${CLICKHOUSE_CLIENT} -q "SELECT * FROM data"

$CLICKHOUSE_CLIENT -q "DROP TABLE data"
