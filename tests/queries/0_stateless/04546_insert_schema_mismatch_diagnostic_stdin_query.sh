#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# Regression test for the case where the INSERT query text is given via --query and the data comes
# from a separate stdin stream, e.g. `clickhouse-client --query "INSERT ... FORMAT TSV" < data.tsv`.
# There, unlike inline data in the query text, ASTInsertQuery::data is null, so the schema-mismatch
# diagnostic has to capture the data as it streams through stdin rather than re-read it from the AST.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

DATA_FILE="${CLICKHOUSE_TMP}/04546_data.tsv"

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_mismatch_stdin_query (c1 Int64, c2 Int64, c3 Int64) ENGINE = Memory"

echo "-- clickhouse-client, --query with data from a separate stdin stream, type mismatch"
printf '1\tpage_view\t/users/profile\n' > "$DATA_FILE"
$CLICKHOUSE_CLIENT --query "INSERT INTO test_mismatch_stdin_query FORMAT TSV" < "$DATA_FILE" 2>&1 | check

echo "-- clickhouse-client, --query with data from a separate stdin stream, matching data (no false positive)"
printf '1\t2\t3\n' > "$DATA_FILE"
$CLICKHOUSE_CLIENT --query "INSERT INTO test_mismatch_stdin_query FORMAT TSV" < "$DATA_FILE" 2>&1 | check

rm -f "$DATA_FILE"
$CLICKHOUSE_CLIENT -q "DROP TABLE test_mismatch_stdin_query"
