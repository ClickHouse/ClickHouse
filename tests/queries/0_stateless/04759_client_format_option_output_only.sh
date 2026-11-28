#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_format_option (a UInt32, b String) ENGINE = Memory"

# In clickhouse-client, `--format` is output-only: it maps to the `output_format` setting
# and must not override the `FORMAT` clause of an INSERT on the input side.
echo '1,"one"' | $CLICKHOUSE_CLIENT --format Pretty -q "INSERT INTO t_format_option FORMAT CSV"
$CLICKHOUSE_CLIENT -q "SELECT * FROM t_format_option"

# ... while it still applies to the output.
$CLICKHOUSE_CLIENT --format CSV -q "SELECT * FROM t_format_option"

# An explicit `--output-format` takes precedence over `--format`.
$CLICKHOUSE_CLIENT --format Pretty --output-format CSV -q "SELECT * FROM t_format_option"

# In clickhouse-local, `--format` keeps its historical bidirectional meaning
# (it maps to the `format` setting): here it sets both the format of the `table`
# read from a file without an extension and the output format.
DATA_FILE="${CLICKHOUSE_TMP}/data_04759"
printf '2,"two"\n' > "$DATA_FILE"
$CLICKHOUSE_LOCAL --file "$DATA_FILE" --format CSV -q "SELECT * FROM table"
rm -f "$DATA_FILE"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_format_option"
