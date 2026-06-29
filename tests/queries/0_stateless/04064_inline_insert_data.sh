#!/usr/bin/env bash
# Tags: no-fasttest

# Test that INSERT with inline data works when the --inline-insert-data flag is used.
# In this mode, the client sends the data as is in the query text instead of converting it to blocks,
# and the server parses the inline data itself.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS test_inline_insert"
$CLICKHOUSE_CLIENT --query "CREATE TABLE test_inline_insert (x UInt64, y String) ENGINE = MergeTree ORDER BY x"

# Test with --inline-insert-data flag
$CLICKHOUSE_CLIENT --inline-insert-data --query "INSERT INTO test_inline_insert VALUES (1, 'hello'), (2, 'world')"
$CLICKHOUSE_CLIENT --inline-insert-data --query "INSERT INTO test_inline_insert FORMAT Values (3, 'foo')"
$CLICKHOUSE_CLIENT --inline-insert-data --query "INSERT INTO test_inline_insert FORMAT JSONEachRow {\"x\": 4, \"y\": \"bar\"}"

# Test with send_table_structure_on_insert_with_inline_data setting directly (without --inline-insert-data flag)
# This verifies the setting-driven path works independently of the CLI flag.
$CLICKHOUSE_CLIENT --send_table_structure_on_insert_with_inline_data 0 --query "INSERT INTO test_inline_insert VALUES (5, 'baz')"
$CLICKHOUSE_CLIENT --send_table_structure_on_insert_with_inline_data 0 --query "INSERT INTO test_inline_insert FORMAT JSONEachRow {\"x\": 6, \"y\": \"setting_json\"}"
$CLICKHOUSE_CLIENT --send_table_structure_on_insert_with_inline_data 0 --query "INSERT INTO test_inline_insert FORMAT CSV
7,\"setting_csv\""

$CLICKHOUSE_CLIENT --query "SELECT * FROM test_inline_insert ORDER BY x"

# Negative test: combining inline insert data with external data from stdin must be rejected.
# This guards against a regression where the new error path silently accepts mixed input.
# We must include explicit inline data after the format so that the parsed query has
# `hasInlinedData() == true` (the parser strips a sole trailing newline after FORMAT,
# which would otherwise make this check non-deterministic under fuzzed settings).
$CLICKHOUSE_CLIENT --inline-insert-data --query "INSERT INTO test_inline_insert FORMAT TSV
100	inline_a" <<<"200	stdin_a" |& grep -F -c 'Processing inline insert data with both inlined and external data (from stdin or infile) is not supported'
$CLICKHOUSE_CLIENT --send_table_structure_on_insert_with_inline_data 0 --query "INSERT INTO test_inline_insert FORMAT TSV
101	inline_b" <<<"201	stdin_b" |& grep -F -c 'Processing inline insert data with both inlined and external data (from stdin or infile) is not supported'

$CLICKHOUSE_CLIENT --query "DROP TABLE test_inline_insert"
