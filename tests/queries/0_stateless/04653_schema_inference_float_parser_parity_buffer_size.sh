#!/usr/bin/env bash
# The inferred type must not depend on how much of the number the working buffer held.
# max_read_buffer_size is not settable per statement, so this needs clickhouse-local.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SMALL=(--storage_file_read_method read --max_read_buffer_size 1 --input-format JSONEachRow)
DEFAULT=(--storage_file_read_method read --input-format JSONEachRow)

echo 'inf: same type at both buffer sizes, and it reads'
$CLICKHOUSE_LOCAL "${SMALL[@]}" 'desc "table"' <<<'{"x" : inf}' | cut -f2
$CLICKHOUSE_LOCAL "${DEFAULT[@]}" 'desc "table"' <<<'{"x" : inf}' | cut -f2
$CLICKHOUSE_LOCAL "${SMALL[@]}" 'select x from "table"' <<<'{"x" : inf}'
$CLICKHOUSE_LOCAL "${DEFAULT[@]}" 'select x from "table"' <<<'{"x" : inf}'

echo 'a partial inf keyword: same type at both buffer sizes'
$CLICKHOUSE_LOCAL "${SMALL[@]}" 'desc "table"' <<<'{"x" : i}' | cut -f2
$CLICKHOUSE_LOCAL "${DEFAULT[@]}" 'desc "table"' <<<'{"x" : i}' | cut -f2

echo 'a later numeric row must not resurrect it, at either buffer size'
$CLICKHOUSE_LOCAL "${SMALL[@]}" 'desc "table"' <<<'{"x" : i}
{"x" : 1.5}' | cut -f2
$CLICKHOUSE_LOCAL "${DEFAULT[@]}" 'desc "table"' <<<'{"x" : i}
{"x" : 1.5}' | cut -f2
