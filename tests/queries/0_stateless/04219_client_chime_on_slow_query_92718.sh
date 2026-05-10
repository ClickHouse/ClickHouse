#!/usr/bin/env bash
# Tags: long
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/92718
# Verifies that `--chime N` makes the client emit ASCII `BEL` (`\x07`) on stderr
# when a query finishes after running for at least N seconds, and stays silent
# otherwise. Works in both success and error paths and applies to clickhouse-client
# and clickhouse-local.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

bel_count() {
    # Count occurrences of ASCII BEL (`\x07`) on stderr; print "BEL" if any, "no BEL" otherwise.
    if grep -q $'\x07' "$1"; then
        echo "BEL"
    else
        echo "no BEL"
    fi
}

err1="${CLICKHOUSE_TMP}/04219_err1_${CLICKHOUSE_DATABASE}.txt"
err2="${CLICKHOUSE_TMP}/04219_err2_${CLICKHOUSE_DATABASE}.txt"
err3="${CLICKHOUSE_TMP}/04219_err3_${CLICKHOUSE_DATABASE}.txt"
err4="${CLICKHOUSE_TMP}/04219_err4_${CLICKHOUSE_DATABASE}.txt"
err5="${CLICKHOUSE_TMP}/04219_err5_${CLICKHOUSE_DATABASE}.txt"

# Case 1: `--chime 1`, slow query (elapsed >= 1.5s) — expect `BEL`.
${CLICKHOUSE_CLIENT} --chime 1 -q "SELECT sleep(1.5) FORMAT Null" 2> "$err1" > /dev/null
echo "1. clickhouse-client --chime 1, slow query: $(bel_count "$err1")"

# Case 2: `--chime 10`, fast query (elapsed ~0.1s) — expect no `BEL` (below threshold).
${CLICKHOUSE_CLIENT} --chime 10 -q "SELECT sleep(0.1) FORMAT Null" 2> "$err2" > /dev/null
echo "2. clickhouse-client --chime 10, fast query: $(bel_count "$err2")"

# Case 3: no `--chime` flag, slow query — expect no `BEL` (chime disabled by default).
${CLICKHOUSE_CLIENT} -q "SELECT sleep(1.5) FORMAT Null" 2> "$err3" > /dev/null
echo "3. clickhouse-client (no --chime), slow query: $(bel_count "$err3")"

# Case 4: `--chime 1`, slow query that ends in an error — expect `BEL` on the error path.
${CLICKHOUSE_CLIENT} --chime 1 -q "SELECT sleep(1.5), throwIf(1 = 1, 'expected error')" 2> "$err4" > /dev/null
echo "4. clickhouse-client --chime 1, slow query then error: $(bel_count "$err4")"

# Case 5: also verify the same behaviour in `clickhouse-local` so the feature is wired
# in `ClientBase` rather than the network client only.
${CLICKHOUSE_LOCAL} --chime 1 -q "SELECT sleep(1.5) FORMAT Null" 2> "$err5" > /dev/null
echo "5. clickhouse-local --chime 1, slow query: $(bel_count "$err5")"

rm -f "$err1" "$err2" "$err3" "$err4" "$err5"
