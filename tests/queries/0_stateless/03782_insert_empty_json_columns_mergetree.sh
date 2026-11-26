#!/usr/bin/env bash
# Tags: no-async-insert
# Regression test: inserting empty data via formats that produce 0-row chunks
# must flow through MergeTreeSink without error. The squashing pipeline in
# PR #94207 changed Squashing::add() to no longer filter 0-row chunks, allowing
# them to reach MergeTreeSink. This test verifies that path stays healthy.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_empty_json_insert (x UInt32) ENGINE = MergeTree ORDER BY x"

# JSONColumnsWithMetadata with empty arrays produces a 0-row truthy chunk.
# Before the squashing fix, this 0-row chunk reached MergeTreeSink unexpectedly.
echo '{"meta":[{"name":"x","type":"UInt32"}],"data":{"x":[]}}' | \
    $CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+test_empty_json_insert+FORMAT+JSONColumnsWithMetadata" --data-binary @-

$CLICKHOUSE_CLIENT -q "SELECT count() FROM test_empty_json_insert"

# JSONColumns with empty arrays — same root cause, different format reader.
echo '{"x":[]}' | \
    $CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+test_empty_json_insert+FORMAT+JSONColumns" --data-binary @-

$CLICKHOUSE_CLIENT -q "SELECT count() FROM test_empty_json_insert"

$CLICKHOUSE_CLIENT -q "DROP TABLE test_empty_json_insert"
