#!/usr/bin/env bash

# Checks that by default a row-based input with wide rows is split into several blocks
# once it exceeds 128 MiB, even though it has far fewer rows than `max_block_size`,
# and that the old behaviour (no byte limit) is restored by `compatibility`.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# 160 rows of 1 MB each: one block by row count, two blocks by the 128 MiB byte limit.
# RowBinary is used because text formats are already split into chunks by parallel parsing.
$CLICKHOUSE_LOCAL -q "SELECT repeat('a', 1000000) FROM numbers(160) FORMAT RowBinary" > $CLICKHOUSE_TEST_UNIQUE_NAME.bin

$CLICKHOUSE_LOCAL -q "SELECT count(DISTINCT blockNumber()), count() FROM file('$CLICKHOUSE_TEST_UNIQUE_NAME.bin', RowBinary, 's String')"
$CLICKHOUSE_LOCAL -q "SELECT count(DISTINCT blockNumber()), count() FROM file('$CLICKHOUSE_TEST_UNIQUE_NAME.bin', RowBinary, 's String') SETTINGS compatibility = '26.10'"

rm $CLICKHOUSE_TEST_UNIQUE_NAME.bin
