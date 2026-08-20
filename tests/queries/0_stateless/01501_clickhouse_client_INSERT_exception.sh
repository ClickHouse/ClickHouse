#!/usr/bin/env bash
# Tags: no-fasttest
# Random settings limits: send_table_structure_on_insert_with_inline_data=(1, 1)
# This test checks for the specific client-side error message
# "data for INSERT was parsed from query" produced when `clickhouse-client` parses inline
# `INSERT FORMAT TSV` data and also receives data on stdin. With
# `send_table_structure_on_insert_with_inline_data=0` the client stops emitting that prefix
# (the parsing moves server-side), so the `grep -F -c` checks in this test return 0 instead of
# 1. Pin the legacy path; this test is about the client-side exception wording, not about which
# side parses inline data.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS data"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE data (key Int) Engine=Memory()"
${CLICKHOUSE_CLIENT} --input_format_parallel_parsing=0 -q "INSERT INTO data SELECT key FROM input('key Int') FORMAT TSV" <<<10
# with '\n...' after the query clickhouse-client prefer data from the query over data from stdin, and produce very tricky message:
#   Code: 27. DB::Exception: Cannot parse input: expected '\n' before: ' ': (at row 1)
# well for TSV it is ok, but for RowBinary:
#   Code: 33. DB::Exception: Cannot read all data. Bytes read: 1. Bytes expected: 4.
# so check that the exception message contain the data source.
${CLICKHOUSE_CLIENT} --async_insert=0 --input_format_parallel_parsing=0 -q "INSERT INTO data FORMAT TSV
 " <<<2 |& grep -F -c 'data for INSERT was parsed from query'
${CLICKHOUSE_CLIENT} --async_insert=1 --input_format_parallel_parsing=0 -q "INSERT INTO data FORMAT TSV
 " <<<2 |& grep -F -c 'Processing async inserts with both inlined and external data (from stdin or infile) is not supported'
${CLICKHOUSE_CLIENT} -q "SELECT * FROM data"

$CLICKHOUSE_CLIENT -q "DROP TABLE data"
