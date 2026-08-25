#!/usr/bin/env bash
# Tests that the ordinal of a bad argument in the "A value of illegal type was provided as ... argument" message
# is one-based and correct, both for the fixed-arity validator and for the variadic one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Extracts just the ordinal from the error message, so the test does not depend on the rest of the wording.
# The server also logs the exception, so the message can appear more than once - only the first one is taken.
ordinal_of_bad_argument()
{
    local ordinal
    ordinal=$($CLICKHOUSE_CLIENT --query "$1" 2>&1 | grep -oP "provided as \K[0-9]+(st|nd|rd|th)" | head -n 1)
    echo "${ordinal:-no match: $1}"
}

# Fixed-arity validator: the bad argument is the 1st, 2nd and 3rd one respectively.
# The 3rd one is an optional argument, so it also covers the mandatory-argument offset.
ordinal_of_bad_argument "SELECT regexpExtract([], 'a')"
ordinal_of_bad_argument "SELECT regexpExtract('a', [])"
ordinal_of_bad_argument "SELECT regexpExtract('a', 'a', [])"

# Variadic validator: the ordinal must not be shifted by the number of mandatory arguments.
ordinal_of_bad_argument "SELECT tupleConcat((1,), 1)"
ordinal_of_bad_argument "SELECT tupleConcat((1,), (2,), 3)"
ordinal_of_bad_argument "SELECT tupleConcat(1, (2,))"

# The special cases of the ordinal endings: 11th, 12th, 13th, then 21st.
ordinal_of_bad_argument "SELECT tupleConcat($(printf '(1,),%.0s' $(seq 1 10))1)"
ordinal_of_bad_argument "SELECT tupleConcat($(printf '(1,),%.0s' $(seq 1 11))1)"
ordinal_of_bad_argument "SELECT tupleConcat($(printf '(1,),%.0s' $(seq 1 12))1)"
ordinal_of_bad_argument "SELECT tupleConcat($(printf '(1,),%.0s' $(seq 1 20))1)"
