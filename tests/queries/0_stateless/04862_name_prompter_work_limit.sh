#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A wide merge() unions the columns of every matching table into one candidate list, and the typo hint
# scores the unresolved name against every candidate. Long names make each comparison expensive, so the
# candidate count and the name length together decide the cost.
NAME_LEN=600
PAD=$(printf 'z%.0s' $(seq 1 $NAME_LEN))

for t in $(seq 0 29); do
    COLS=""
    for c in $(seq 0 39); do
        base=$(printf 'col_%03d_%03d_' "$t" "$c")
        COLS="${COLS}${COLS:+, }${base}${PAD:0:$((NAME_LEN - ${#base}))} UInt64"
    done
    echo "CREATE TABLE t_04862_${t} (${COLS}) ENGINE = Memory;"
done | $CLICKHOUSE_CLIENT --max_query_size 100000000 -n

# Several candidates are equally close to this name, so which one wins depends on iteration order. The
# assertions below therefore check only whether a suggestion is offered, never which name it names.
UNRESOLVED="col_999_999_${PAD:0:$((NAME_LEN - 12))}"

# One table: 40 candidates are affordable, so the hint is offered.
echo "SELECT ${UNRESOLVED} FROM t_04862_0" \
    | $CLICKHOUSE_CLIENT --max_query_size 100000000 2>&1 \
    | tr '\n' ' ' | grep -c -e 'Maybe you meant'

# All thirty: 1200 candidates of this length cost more than one hint computation is allowed to spend, so
# no hint is offered. The identifier error itself is unchanged.
echo "SELECT ${UNRESOLVED} FROM merge(currentDatabase(), '^t_04862_')" \
    | $CLICKHOUSE_CLIENT --max_query_size 100000000 2>&1 \
    | tr '\n' ' ' | grep -c -e 'Maybe you meant' || true
echo "SELECT ${UNRESOLVED} FROM merge(currentDatabase(), '^t_04862_')" \
    | $CLICKHOUSE_CLIENT --max_query_size 100000000 2>&1 \
    | tr '\n' ' ' | grep -c -e UNKNOWN_IDENTIFIER

for t in $(seq 0 29); do
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04862_${t}"
done
