#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Scoring one typo-hint candidate costs the product of the two name lengths, so the candidate count and
# the name length together decide the cost of one hint computation. With 600-character names each
# candidate costs 360,000 cells, so the number of columns places a table just under or just over the
# budget: 138 columns is 49,680,000 cells and 139 is 50,040,000.
NAME_LEN=600
PAD=$(printf 'z%.0s' $(seq 1 $NAME_LEN))

# Both analyzers render the suggestion with a different capitalisation, and this test greps for it, so
# every asserting query below pins the analyzer explicitly.
CLIENT="$CLICKHOUSE_CLIENT --enable_analyzer=1 --max_query_size 200000000"

make_table() {
    local name=$1 count=$2 cols="" c base
    for c in $(seq 0 $((count - 1))); do
        base=$(printf 'c%05d_' "$c")
        cols="${cols}${cols:+, }${base}${PAD:0:$((NAME_LEN - ${#base}))} UInt64"
    done
    echo "CREATE TABLE ${name} (${cols}) ENGINE = Memory;" | $CLICKHOUSE_CLIENT --max_query_size 200000000
}

make_table t_04862_under 138
make_table t_04862_over 139

# Several candidates are equally close to this name, so which one wins depends on iteration order. The
# assertions below therefore only check whether a suggestion is offered, never which name it names.
UNRESOLVED="col_999_999_${PAD:0:$((NAME_LEN - 12))}"

hint_offered() {
    echo "SELECT ${UNRESOLVED} FROM $1" | $CLIENT 2>&1 \
        | tr '\n' ' ' | grep -c -e 'Maybe you meant' || true
}

# Just under the budget the hint is still offered, and just over it the hint is declined. The pair
# brackets the budget closely enough that halving or doubling it changes one of these two lines.
hint_offered t_04862_under
hint_offered t_04862_over

# The unresolved-name error itself is unchanged when the hint is declined.
echo "SELECT ${UNRESOLVED} FROM t_04862_over" | $CLIENT 2>&1 \
    | tr '\n' ' ' | grep -c -e UNKNOWN_IDENTIFIER

# A cancelled query stops instead of computing hints for a result nobody will read. One hint
# computation per unresolved name is charged its own budget, so 60 unresolved names cost far more than
# the deadline allows however fast the machine is; only the error code is asserted, never a duration.
# The old analyzer is pinned here because it is the one that computes a hint per unresolved name.
SEL=""
for i in $(seq 0 59); do
    n=$(printf 'u%05d_' "$i")
    SEL="${SEL}${SEL:+, }${n}${PAD:0:$((NAME_LEN - ${#n}))}"
done
echo "SELECT ${SEL} FROM t_04862_over" \
    | $CLICKHOUSE_CLIENT --enable_analyzer=0 --max_query_size 200000000 --max_execution_time=1 2>&1 \
    | tr '\n' ' ' | grep -c -e TIMEOUT_EXCEEDED || true

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04862_under"
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04862_over"
