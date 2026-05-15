#!/usr/bin/env bash
# Tags: no-fasttest
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/101748
# The `--reconnect` option used to be a boolean flag (no value). After it was
# changed to an integer option it lost `implicit_value(1)`, breaking the old
# bare `--reconnect` syntax with: "the required argument for option
# '--reconnect' is missing". The fix re-adds `implicit_value(1)` so all three
# forms work: `--reconnect`, `--reconnect=0`, `--reconnect=N`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `--reconnect` with no value must parse successfully and run the query.
$CLICKHOUSE_BENCHMARK --iterations=1 --reconnect <<< 'SELECT 1' >/dev/null 2>&1
echo "bare: $?"

# `--reconnect=0` (explicit zero) must also parse successfully.
$CLICKHOUSE_BENCHMARK --iterations=1 --reconnect=0 <<< 'SELECT 1' >/dev/null 2>&1
echo "zero: $?"

# `--reconnect=3` (explicit N) must also parse successfully.
$CLICKHOUSE_BENCHMARK --iterations=1 --reconnect=3 <<< 'SELECT 1' >/dev/null 2>&1
echo "n: $?"
