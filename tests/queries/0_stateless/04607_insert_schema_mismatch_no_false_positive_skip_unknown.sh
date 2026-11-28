#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# https://github.com/ClickHouse/ClickHouse/issues/110622
# Formats with named fields (here: JSONEachRow) can carry fields unknown to the destination table.
# With input_format_skip_unknown_fields = 1 (the default) the parser legally skips them, so an extra
# field is not a structure mismatch and must not add a misleading "structure mismatch" suffix to an
# otherwise unrelated parse error. With input_format_skip_unknown_fields = 0 the parser rejects the
# row precisely because of the unknown field, and there the differing structure is worth explaining.

PHRASE="does not match the structure expected by the query"

check() {
    local out
    out=$(cat)
    if echo "$out" | grep -q "Code:"; then echo "insert failed as expected"; else echo "insert unexpectedly succeeded"; fi
    if echo "$out" | grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

$CLICKHOUSE_CLIENT -q "CREATE TABLE test_mismatch_skip_unknown (a UInt8) ENGINE = Memory"

echo "-- a skipped unknown field is not a mismatch; the parse fails only on the fractional value"
printf 'INSERT INTO test_mismatch_skip_unknown FORMAT JSONEachRow\n{"a": 1.5, "extra": "x"}\n' \
    | $CLICKHOUSE_CLIENT --async_insert 0 --input_format_skip_unknown_fields 1 2>&1 | check

echo "-- with skipping disabled the parse fails on the unknown field itself; the differing structure is explained"
printf 'INSERT INTO test_mismatch_skip_unknown FORMAT JSONEachRow\n{"a": 1, "extra": "x"}\n' \
    | $CLICKHOUSE_CLIENT --async_insert 0 --input_format_skip_unknown_fields 0 2>&1 | check

$CLICKHOUSE_CLIENT -q "DROP TABLE test_mismatch_skip_unknown"
