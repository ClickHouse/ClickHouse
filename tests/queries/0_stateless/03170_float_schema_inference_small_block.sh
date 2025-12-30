#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# do not fallback to float always
echo "Int64"
$CLICKHOUSE_LOCAL --storage_file_read_method read --max_read_buffer_size 1 --input-format JSONEachRow 'desc "table"' <<<'{"x" : 1}'
$CLICKHOUSE_LOCAL --storage_file_read_method read --max_read_buffer_size 1 --input-format JSONEachRow 'desc "table"' <<<'{"x" : +1}'
$CLICKHOUSE_LOCAL --storage_file_read_method read --max_read_buffer_size 1 --input-format JSONEachRow 'desc "table"' <<<'{"x" : -1}'

echo "Float64"
$CLICKHOUSE_LOCAL --storage_file_read_method read --max_read_buffer_size 1 --input-format JSONEachRow 'desc "table"' <<<'{"x" : 1.1}'
$CLICKHOUSE_LOCAL --storage_file_read_method read --max_read_buffer_size 1 --input-format JSONEachRow 'desc "table"' <<<'{"x" : +1.1}'
$CLICKHOUSE_LOCAL --storage_file_read_method read --max_read_buffer_size 1 --input-format JSONEachRow 'desc "table"' <<<'{"x" : 1.111}'
$CLICKHOUSE_LOCAL --storage_file_read_method read --max_read_buffer_size 1 --input-format JSONEachRow 'desc "table"' <<<'{"x" : +1.111}'

# this is requried due to previously clickhouse-local does not interprets
# --max_read_buffer_size for fds [1]
#
#   [1]: https://github.com/ClickHouse/ClickHouse/pull/64532
echo "Float64.explicit File"
tmp_path=$(mktemp "$CUR_DIR/03170_float_schema_inference_small_block.json.XXXXXX")
trap 'rm -f $tmp_path' EXIT
cat > "$tmp_path" <<<'{"x" : 1.111}'
$CLICKHOUSE_LOCAL --storage_file_read_method read --max_read_buffer_size 1 --input-format JSONEachRow 'desc "table"' --file "$tmp_path"

echo "Float64.pipe"
echo '{"x" : 1.1}' | $CLICKHOUSE_LOCAL --storage_file_read_method read --max_read_buffer_size 1 --input-format JSONEachRow 'desc "table"'
echo "Float64.default max_read_buffer_size"
echo '{"x" : 1.1}' | $CLICKHOUSE_LOCAL --storage_file_read_method read --input-format JSONEachRow 'desc "table"'
