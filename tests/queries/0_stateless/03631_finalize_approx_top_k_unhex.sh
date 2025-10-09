#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

QUERY="SELECT finalizeAggregation(CAST(unhex('012A0300000000000000030000000000000043434303000000000000004141410400000000000000414141410100800200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000') AS AggregateFunction(approx_top_k(3), String)))"

out="$( { printf '%s\n' "$QUERY"; } | ${CLICKHOUSE_CLIENT} -n 2>&1 )"
status=$?

if [ "$status" -eq 0 ] && echo "$out" | grep -qE '^\s*\[\]\s*$'; then
  echo OK
else
  echo "$out"
  exit 1
fi

# Happy-case genuine state must finalize to non-empty deterministic result
HAPPY_QUERY="SELECT finalizeAggregation(approx_top_kState(3)(x)) FROM (SELECT arrayJoin([1,1,2,2,2,3]) AS x)"
happy_out="$( { printf '%s\n' "$HAPPY_QUERY"; } | ${CLICKHOUSE_CLIENT} -n 2>&1 )"
happy_status=$?

if [ "$happy_status" -eq 0 ] && echo "$happy_out" | grep -qE '^\s*\[\(2,3,0\),\(1,2,0\),\(3,1,0\)\]\s*$'; then
  echo OK
  exit 0
else
  echo "$happy_out"
  exit 1
fi


