#!/usr/bin/env bash

# A numeric literal is resolved to its concrete value before a literal array's column name is
# synthesized, so two spellings of the same value (here 1e9 and 1000000000.) produce the same
# column name. This must hold for an array of any size, otherwise the name differs from the one an
# older server (which parses the literal straight to Float64) produces, breaking block-structure
# matching in mixed-version distributed queries.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

column_name() {
    $CLICKHOUSE_CLIENT -q "SELECT [$1] FORMAT TSVWithNames" | sed -n '1p'
}

compare() {
    local exponent decimal
    exponent=$(column_name "$2")
    decimal=$(column_name "$3")
    if [ "$exponent" = "$decimal" ]
    then
        echo "$1 -> same"
    else
        echo "$1 -> differ: $exponent vs $decimal"
    fi
}

# More than 100 elements, and 100 elements or fewer: the two spellings agree in both cases.
large_exponent=$(yes '1e9' | head -n 101 | paste -sd,)
large_decimal=$(yes '1000000000.' | head -n 101 | paste -sd,)
compare "large" "$large_exponent" "$large_decimal"
compare "small" "1e9, 1e9" "1000000000., 1000000000."
