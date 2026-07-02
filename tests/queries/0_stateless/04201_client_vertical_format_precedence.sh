#!/usr/bin/env bash

# Test: precedence of --output-format/--format over --vertical for clickhouse-client and clickhouse-local.
# Covers: src/Client/ClientBase.cpp:917-931 — the if-else chain in setDefaultFormatsAndCompressionFromConfiguration
# that gives priority to --output-format > --format > --vertical when determining default_output_format.
# A reorder of these branches (e.g., putting `vertical` first) would cause user-specified --format to be ignored.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# --output-format wins over --vertical (both should produce CSV, NOT Vertical):
echo "--- local: --vertical --output-format CSV ---"
${CLICKHOUSE_LOCAL} --vertical --output-format CSV --query "SELECT 'A' AS a, 'B' AS b"

echo "--- client: --vertical --output-format CSV ---"
${CLICKHOUSE_CLIENT} --vertical --output-format CSV --query "SELECT 'A' AS a, 'B' AS b"

# --format wins over --vertical (both should produce JSONEachRow, NOT Vertical):
echo "--- local: --vertical --format JSONEachRow ---"
${CLICKHOUSE_LOCAL} --vertical --format JSONEachRow --query "SELECT 'A' AS a, 'B' AS b"

echo "--- client: --vertical --format JSONEachRow ---"
${CLICKHOUSE_CLIENT} --vertical --format JSONEachRow --query "SELECT 'A' AS a, 'B' AS b"

# --output-format wins over --format (sanity, already covered by 02206 but combine with --vertical):
echo "--- local: --vertical --output-format CSV --format JSONEachRow ---"
${CLICKHOUSE_LOCAL} --vertical --output-format CSV --format JSONEachRow --query "SELECT 'A' AS a, 'B' AS b"

echo "--- client: --vertical --output-format CSV --format JSONEachRow ---"
${CLICKHOUSE_CLIENT} --vertical --output-format CSV --format JSONEachRow --query "SELECT 'A' AS a, 'B' AS b"
