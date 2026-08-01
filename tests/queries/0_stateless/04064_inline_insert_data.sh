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

# INSERT ... SELECT does not carry inline data (the rows come from the SELECT), so the
# new inline path is bypassed regardless of the flag/setting (see `!insert->select` in
# `is_inline_insert_data`). These cases verify that requesting inline insert data mode
# does not break SELECT-driven INSERTs.
$CLICKHOUSE_CLIENT --inline-insert-data --query "INSERT INTO test_inline_insert SELECT number + 8 AS x, concat('select_', toString(number)) AS y FROM numbers(2)"
$CLICKHOUSE_CLIENT --send_table_structure_on_insert_with_inline_data 0 --query "INSERT INTO test_inline_insert SELECT number + 10 AS x, concat('select_setting_', toString(number)) AS y FROM numbers(2)"

# `async_insert = 0` combined with inline insert data mode. The explicit user choice
# (CLI flag or setting) takes precedence over `async_insert` on the client side, so the
# data is parsed by the server inline regardless of the async insert toggle.
$CLICKHOUSE_CLIENT --async_insert 0 --inline-insert-data --query "INSERT INTO test_inline_insert VALUES (12, 'no_async')"
$CLICKHOUSE_CLIENT --async_insert 0 --send_table_structure_on_insert_with_inline_data 0 --query "INSERT INTO test_inline_insert FORMAT JSONEachRow {\"x\": 13, \"y\": \"no_async_setting\"}"

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

# Path-sensitive assertion for the `async_insert = 0` combination: the positive cases above
# only prove that rows are inserted, which would still pass if the client regressed to the
# legacy local-parsing path. The inline-vs-external rejection only fires when the inline path
# is actually taken (`is_inline_insert_data == true`), so these cases prove that the explicit
# inline choice still wins over `async_insert = 0` rather than silently falling back.
$CLICKHOUSE_CLIENT --async_insert 0 --inline-insert-data --query "INSERT INTO test_inline_insert FORMAT TSV
102	inline_c" <<<"202	stdin_c" |& grep -F -c 'Processing inline insert data with both inlined and external data (from stdin or infile) is not supported'
$CLICKHOUSE_CLIENT --async_insert 0 --send_table_structure_on_insert_with_inline_data 0 --query "INSERT INTO test_inline_insert FORMAT TSV
103	inline_d" <<<"203	stdin_d" |& grep -F -c 'Processing inline insert data with both inlined and external data (from stdin or infile) is not supported'

$CLICKHOUSE_CLIENT --query "DROP TABLE test_inline_insert"
