#!/usr/bin/env bash

# A numeric literal is resolved to its concrete value before a literal array's column name is
# synthesized, so two spellings of the same value (here 1e9 and 1000000000.) produce the same
# column name. This must hold both for the direct path and for the hashed path used when an array
# has more than 100 elements, otherwise the name differs from the one an older server (which parses
# the literal straight to Float64) produces, breaking block-structure matching in mixed-version
# distributed queries. The column name here comes from the AST (old analyzer path).
#
# Each line below prints the synthesized column name; the two spellings of each array must print
# the same name.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

show() {
    echo -n "$1 -> "
    $CLICKHOUSE_CLIENT --enable_analyzer=0 -q "SELECT [$2] FORMAT TSVWithNames" | sed -n '1p'
}

# More than 100 elements: the column name is a hash of the (resolved) array elements.
large_exponent=$(yes '1e9' | head -n 101 | paste -sd,)
large_decimal=$(yes '1000000000.' | head -n 101 | paste -sd,)
show "large exponent" "$large_exponent"
show "large decimal " "$large_decimal"

# 100 elements or fewer: the column name lists the (resolved) elements directly.
show "small exponent" "1e9, 1e9"
show "small decimal " "1000000000., 1000000000."
