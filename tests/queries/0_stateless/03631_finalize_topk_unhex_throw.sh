#!/usr/bin/env bash
# Tags: no-parallel

# Purpose: Regression/safety test for topK/approx_top_k state deserialization and finalization
# - Verifies finalizeAggregation() on a malformed external state for non-plain types fails (throws) instead of crashing
# - Confirms a valid in-process state still finalizes deterministically
# Check this: https://github.com/ClickHouse/ClickHouse/issues/86882

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Malformed user-provided state (taken from the issue description)
STATE_HEX="012A0300000000000000030000000000000043434303000000000000004141410400000000000000414141410100800200000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"

# Expect both approx_top_k and topK to throw on finalize from untrusted state for non-plain types.
queries=(
  "SELECT finalizeAggregation(CAST(unhex('${STATE_HEX}'), 'AggregateFunction(approx_top_k(3), Array(Array(String)))'))"
  "SELECT finalizeAggregation(CAST(unhex('${STATE_HEX}'), 'AggregateFunction(topK(3), Array(Array(String)))'))"
)

ok=1
for q in "${queries[@]}"; do
  out=$( { printf '%s\n' "$q"; } | ${CLICKHOUSE_CLIENT} -n 2>&1 )
  status=$?
  if [ "$status" -ne 0 ]; then
    :
  else
    echo "Unexpected success for: $q" >&2
    echo "$out" >&2
    ok=0
  fi
done

# Happy-case genuine state must finalize to deterministic non-empty result
HAPPY_QUERY="SELECT finalizeAggregation(approx_top_kState(3)(x)) FROM (SELECT arrayJoin([1,1,2,2,2,3]) AS x)"
happy_out=$( { printf '%s\n' "$HAPPY_QUERY"; } | ${CLICKHOUSE_CLIENT} -n 2>&1 )
happy_status=$?
if [ "$happy_status" -eq 0 ] && echo "$happy_out" | grep -qE '^\s*\[\(2,3,0\),\(1,2,0\),\(3,1,0\)\]\s*$'; then
  :
else
  echo "$happy_out" >&2
  ok=0
fi

if [ "$ok" -eq 1 ]; then
  echo OK
else
  exit 1
fi
