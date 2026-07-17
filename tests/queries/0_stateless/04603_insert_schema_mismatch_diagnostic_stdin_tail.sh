#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# Regression test for the mixed-source path: the INSERT query text given via --query already carries a
# valid inline data prefix, and more data is streamed separately from stdin. The client parses the inline
# prefix first, then re-enters sendDataFrom for the stdin tail reusing the same ASTInsertQuery. When the
# parse error happens in the streamed tail, the diagnostic must inspect the stdin bytes (via the prefix
# capturing read buffer), not the already-consumed inline prefix from the AST.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

DATA_FILE="${CLICKHOUSE_TMP}/04603_data.tsv"

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_mismatch_stdin_tail (c1 Int64, c2 Int64, c3 Int64) ENGINE = Memory"

echo "-- inline prefix parses fine, the streamed stdin tail has a type mismatch"
printf '10\tpage_view\t/users/profile\n' > "$DATA_FILE"
$CLICKHOUSE_CLIENT --async_insert 0 --query $'INSERT INTO test_mismatch_stdin_tail FORMAT TSV\n1\t2\t3' < "$DATA_FILE" 2>&1 | check

echo "-- inline prefix parses fine, the streamed stdin tail also matches (no false positive)"
printf '10\t20\t30\n' > "$DATA_FILE"
$CLICKHOUSE_CLIENT --async_insert 0 --query $'INSERT INTO test_mismatch_stdin_tail FORMAT TSV\n1\t2\t3' < "$DATA_FILE" 2>&1 | check

rm -f "$DATA_FILE"
$CLICKHOUSE_CLIENT -q "DROP TABLE test_mismatch_stdin_tail"
