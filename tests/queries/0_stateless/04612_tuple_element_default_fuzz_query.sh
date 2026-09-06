#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Regression test: `fuzzQuery` must accept valid DDL statements that use `DEFAULT` expressions
# inside `Tuple` data types. The fuzzer used to construct a real data type from the raw column
# type AST before the tuple element defaults were normalized away, which threw `BAD_ARGUMENTS`
# ("Data type Tuple cannot have a DEFAULT expression for element ...").
#
# The regression did not depend on the seed at all - the type was reified before any fuzzing
# happened - so a handful of seeds per statement is enough; a larger sweep only made the test
# exceed the 180 second limit under sanitizers.
#
# The fuzzer itself invents random types, and for some seeds those are invalid on their own (a
# doubly nested `Nullable`, a `FixedString` without a length, an `Enum` with duplicate names, ...),
# which fails the query with an error that has nothing to do with this feature and changes as the
# fuzzer and the set of data types evolve. Such failures cannot be enumerated, so instead of
# matching one error message the test asserts two things that do not depend on what the fuzzer
# invents: the statement itself still parses with the element `DEFAULT` intact, and `fuzzQuery`
# accepts it for at least one seed (the regression made it fail for every seed).

queries=(
    "CREATE TABLE t (c Tuple(a UInt8 DEFAULT 1, s String DEFAULT 'Hello')) ENGINE = Memory"
    "ALTER TABLE t ADD COLUMN c Tuple(a UInt8, s String DEFAULT 'Hello')"
    "CREATE TABLE t (c Tuple(a UInt8, t Tuple(b String DEFAULT 'x', c UInt8))) ENGINE = Memory"
    "CREATE TABLE t (c Nullable(Tuple(a UInt8 DEFAULT 1))) ENGINE = Memory"
)

for query in "${queries[@]}"; do
    # A parser regression (for example a `SYNTAX_ERROR` at the `DEFAULT` token) shows up here.
    if ! formatted=$($CLICKHOUSE_CLIENT --param_query "$query" --query "SELECT formatQuery({query:String})" 2>&1)
    then
        echo "FAIL (does not parse): ${query}"
        echo "$formatted"
        continue
    fi
    if ! echo "$formatted" | grep -qF 'DEFAULT'; then
        echo "FAIL (element DEFAULT lost while formatting): ${query}"
        echo "$formatted"
        continue
    fi

    accepted=0
    for seed in {1..5}; do
        if output=$($CLICKHOUSE_CLIENT --param_query "$query" --query \
            "SELECT * FROM fuzzQuery({query:String}, 500, ${seed}) LIMIT 10 FORMAT Null" 2>&1)
        then
            accepted=1
            continue
        fi

        # This one is never acceptable: it is the regression itself.
        if echo "$output" | grep -qF 'DEFAULT expression for element'; then
            echo "FAIL (seed ${seed}): ${query}"
            echo "$output"
        fi
    done

    if [[ $accepted -eq 0 ]]; then
        echo "FAIL (rejected for every seed): ${query}"
        echo "$output"
    fi
done

echo 'OK'
