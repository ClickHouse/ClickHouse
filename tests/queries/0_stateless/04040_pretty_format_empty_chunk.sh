#!/usr/bin/env bash
# Regression test: Pretty format with 0-row chunks from IStorageSystemOneBlock
# should not throw std::length_error.
# https://github.com/ClickHouse/ClickHouse/issues/99528

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# system.asynchronous_inserts uses IStorageSystemOneBlock which produced
# a 0-row chunk with non-empty columns when the table was empty,
# triggering log10(0) in Pretty format's calculateWidths.
# The table may or may not have data in CI, so we only check for no errors.
${CLICKHOUSE_CLIENT} --query "SELECT 1 FROM system.asynchronous_inserts FORMAT Pretty" > /dev/null
${CLICKHOUSE_CLIENT} --query "SELECT 1 FROM system.asynchronous_inserts FORMAT PrettyCompact" > /dev/null
${CLICKHOUSE_CLIENT} --query "SELECT 1 FROM system.asynchronous_inserts FORMAT PrettySpace" > /dev/null

echo "OK"
