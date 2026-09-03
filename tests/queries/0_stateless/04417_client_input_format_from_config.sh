#!/usr/bin/env bash

# An `input-format` supplied via the client config file (or a named connection) must reach the active
# client context that the native INSERT readers consult — not only `cmd_settings`. Otherwise a
# config-driven `input-format` is silently ignored while the equivalent `--input-format` works. Here the
# INSERT declares `FORMAT TabSeparated`, but the config `input-format=CSV` overrides it (matching the
# input-format precedence over the FORMAT clause), so the comma-separated stdin data parses as CSV.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config="${CLICKHOUSE_TMP}/04417_client_input_format.xml"
cat > "$config" <<EOL
<clickhouse>
    <input-format>CSV</input-format>
</clickhouse>
EOL

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_in"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_in (a UInt8, b UInt8) ENGINE = Memory"

echo "-- config <input-format>CSV</input-format> is honored by the native client INSERT reader (data is CSV, FORMAT clause says TabSeparated)"
printf '1,2\n3,4\n' | ${CLICKHOUSE_CLIENT} --config-file="$config" -q "INSERT INTO t_in FORMAT TabSeparated"
${CLICKHOUSE_CLIENT} -q "SELECT a, b FROM t_in ORDER BY a"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_in"
rm -f "$config"
