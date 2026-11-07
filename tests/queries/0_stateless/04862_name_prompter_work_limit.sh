#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Scoring one candidate costs the product of the two name lengths and the budget is charged for every
# candidate, so a column count places a table just under or just over it: at 300 characters each
# candidate costs 90,000 cells, so 555 columns are 49,950,000 and 556 are 50,040,000. No single
# candidate comes close to the budget on its own, which is what makes these two lines depend on the
# charging being cumulative. Halving or doubling the budget moves one of them.
NAME_LEN=300
UNDER_COLUMNS=555
OVER_COLUMNS=556

# Both analyzers render the suggestion with a different capitalisation, and this test greps for it, so
# every asserting query below pins the analyzer explicitly.
CLIENT="$CLICKHOUSE_CLIENT --enable_analyzer=1 --max_query_size 200000000"

pad() { printf 'z%.0s' $(seq 1 "$1"); }
PAD=$(pad $NAME_LEN)

make_table() {
    local name=$1 count=$2 cols="" c base
    for c in $(seq 0 $((count - 1))); do
        base=$(printf 'c%05d_' "$c")
        cols="${cols}${cols:+, }${base}${PAD:0:$((NAME_LEN - ${#base}))} UInt64"
    done
    echo "CREATE TABLE ${name} (${cols}) ENGINE = Memory;" | $CLICKHOUSE_CLIENT --max_query_size 200000000
}

make_table t_04862_under $UNDER_COLUMNS
make_table t_04862_over $OVER_COLUMNS

# Several candidates are equally close to this name, so which one wins depends on iteration order. The
# assertions below therefore only check whether a suggestion is offered, never which name it names. The
# candidates also have to be length-comparable to it or they are rejected before the budget is consulted.
UNRESOLVED="q_${PAD:0:$((NAME_LEN - 2))}"

# Just under the budget the hint is still offered, and just over it the hint is declined. The second
# query also carries the unresolved-name error itself, which the hint limit must leave unchanged, so
# both properties are asserted from one execution of it.
echo "SELECT ${UNRESOLVED} FROM t_04862_under" | $CLIENT 2>&1 \
    | tr '\n' ' ' | grep -c -e 'Maybe you meant' || true

OVER_ERROR=$(echo "SELECT ${UNRESOLVED} FROM t_04862_over" | $CLIENT 2>&1 | tr '\n' ' ')
echo "$OVER_ERROR" | grep -c -e 'Maybe you meant' || true
echo "$OVER_ERROR" | grep -c -e UNKNOWN_IDENTIFIER

# A cancelled query stops instead of computing hints for a result nobody will read. Each unresolved name
# is charged its own budget and the deadline is only observed between those computations, so this needs
# the opposite shape to the two lines above: a total far beyond what any machine can finish within the
# deadline, out of computations each small enough not to overshoot it for long. 600 names against 14
# candidates of 600 characters is 3,024,000,000 cells in computations of 5,040,000. Only the error code
# is asserted, never a duration. The old analyzer is pinned here because it is the one that computes a
# hint per unresolved name.
CANCEL_LEN=600
CANCEL_PAD=$(pad $CANCEL_LEN)
COLS=""
for c in $(seq 0 13); do
    base=$(printf 'c%05d_' "$c")
    COLS="${COLS}${COLS:+, }${base}${CANCEL_PAD:0:$((CANCEL_LEN - ${#base}))} UInt64"
done
echo "CREATE TABLE t_04862_cancel (${COLS}) ENGINE = Memory;" | $CLICKHOUSE_CLIENT --max_query_size 200000000

SEL=""
for i in $(seq 0 599); do
    n=$(printf 'u%05d_' "$i")
    SEL="${SEL}${SEL:+, }${n}${CANCEL_PAD:0:$((CANCEL_LEN - ${#n}))}"
done
echo "SELECT ${SEL} FROM t_04862_cancel" \
    | $CLICKHOUSE_CLIENT --enable_analyzer=0 --max_query_size 200000000 --max_execution_time=1 2>&1 \
    | tr '\n' ' ' | grep -c -e TIMEOUT_EXCEEDED || true

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04862_under"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04862_over"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04862_cancel"
