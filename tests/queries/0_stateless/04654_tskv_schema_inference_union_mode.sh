#!/usr/bin/env bash
# Tags: no-fasttest

# Union mode merges the per-file schemas through a fresh stateless schema reader, so the per-file
# inference provenance (which Int64 was inferred from a negative literal) is gone by then. Without it a
# sign-dependent Int64 to UInt64 widening cannot be proven safe, so it is declined and the caller
# reports the type mismatch instead of inferring a type whose read then fails. Needs real files, which
# is why this is a shell test and not part of 04653.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DIR=$CLICKHOUSE_TEST_UNIQUE_NAME
rm -rf "$DIR"
mkdir -p "$DIR"

printf 'x=-1\n' > "$DIR/neg.tskv"
printf 'x=-2\n' > "$DIR/neg2.tskv"
printf 'x=1\n' > "$DIR/pos.tskv"
printf 'x=18446744073709551615\n' > "$DIR/big.tskv"
printf 'x=1.5\n' > "$DIR/float.tskv"

# Report either the inferred type or the error name, never both: the error message text also contains a
# type name, and it carries the temporary file path, which is not reproducible.
verdict() {
    local out
    out=$($CLICKHOUSE_LOCAL -m -q "$1" 2>&1)
    if echo "$out" | grep -q 'TYPE_MISMATCH'; then
        echo "TYPE_MISMATCH"
    elif echo "$out" | grep -q 'CANNOT_PARSE_INPUT_ASSERTION_FAILED'; then
        echo "CANNOT_PARSE_INPUT_ASSERTION_FAILED"
    else
        echo "$out" | cut -f2- | tr -d '\t'
    fi
}

echo "1. a negative integer and a UInt64-range value in separate files"
# Not a schema whose read then fails: a loud refusal at inference time is the acceptable outcome.
verdict "set schema_inference_mode='union'; desc file('$DIR/{neg,big}.tskv', TSKV);"
verdict "set schema_inference_mode='union'; select * from file('$DIR/{neg,big}.tskv', TSKV) order by tuple(*);"

echo "2. the same shape without a negative value is refused identically, as before this change"
verdict "set schema_inference_mode='union'; desc file('$DIR/{pos,big}.tskv', TSKV);"

echo "3. merges that do not depend on the sign of an integer still happen"
verdict "set schema_inference_mode='union'; desc file('$DIR/{neg,neg2}.tskv', TSKV);"
verdict "set schema_inference_mode='union'; select * from file('$DIR/{neg,neg2}.tskv', TSKV) order by tuple(*);"
verdict "set schema_inference_mode='union'; desc file('$DIR/{neg,float}.tskv', TSKV);"
verdict "set schema_inference_mode='union'; select * from file('$DIR/{neg,float}.tskv', TSKV) order by tuple(*);"

echo "4. the default (non-union) mode on the same two files is unaffected"
verdict "set schema_inference_mode='default'; desc file('$DIR/{neg,big}.tskv', TSKV);"
verdict "set schema_inference_mode='default'; select * from file('$DIR/{neg,big}.tskv', TSKV) order by tuple(*);"

rm -rf "$DIR"
