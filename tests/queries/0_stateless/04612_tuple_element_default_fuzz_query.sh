#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Regression test: `fuzzQuery` must accept valid DDL statements that use `DEFAULT` expressions
# inside `Tuple` data types. The fuzzer used to construct a real data type from the raw column
# type AST before the tuple element defaults were normalized away, which threw `BAD_ARGUMENTS`
# ("Data type Tuple cannot have a DEFAULT expression for element ...").
#
# For some seeds the fuzzer produces a nonsensical type on its own (e.g. a doubly nested
# `Nullable`) and fails with an unrelated error - that happens with or without a tuple element
# `DEFAULT` and is out of scope here, so only the tuple-default error is treated as a failure.
#
# The regression did not depend on the seed at all - the type was reified before any fuzzing
# happened - so a handful of seeds per statement is enough; a larger sweep only made the test
# exceed the 180 second limit under sanitizers.

queries=(
    "CREATE TABLE t (c Tuple(a UInt8 DEFAULT 1, s String DEFAULT 'Hello')) ENGINE = Memory"
    "ALTER TABLE t ADD COLUMN c Tuple(a UInt8, s String DEFAULT 'Hello')"
    "CREATE TABLE t (c Tuple(a UInt8, t Tuple(b String DEFAULT 'x', c UInt8))) ENGINE = Memory"
    "CREATE TABLE t (c Nullable(Tuple(a UInt8 DEFAULT 1))) ENGINE = Memory"
)

for query in "${queries[@]}"; do
    for seed in {1..5}; do
        $CLICKHOUSE_CLIENT --param_query "$query" --query \
            "SELECT * FROM fuzzQuery({query:String}, 500, ${seed}) LIMIT 10 FORMAT Null" 2>&1 \
            | grep -F 'DEFAULT expression for element' | sed "s/^/FAIL (seed ${seed}): /"
    done
done

echo 'OK'
