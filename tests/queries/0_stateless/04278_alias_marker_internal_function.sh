#!/usr/bin/env bash
# Tags: distributed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "---- function exists and forwards arg0 ----"
$CLICKHOUSE_CLIENT --query "SELECT __aliasMarker(42, 'anything')"
$CLICKHOUSE_CLIENT --query "SELECT __aliasMarker(1 + number, 't.x') FROM numbers(3) ORDER BY 1"

echo "---- setting enable_alias_marker exists ----"
$CLICKHOUSE_CLIENT --query "SELECT name, type FROM system.settings WHERE name = 'enable_alias_marker'"

echo "---- lambda over Distributed: no LOGICAL_ERROR ----"
$CLICKHOUSE_CLIENT --multiquery <<'EOF'
DROP TABLE IF EXISTS t_local_04278_lambda;
DROP TABLE IF EXISTS t_dist_04278_lambda;
CREATE TABLE t_local_04278_lambda (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_local_04278_lambda VALUES (1), (2), (3);
CREATE TABLE t_dist_04278_lambda AS t_local_04278_lambda
ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_local_04278_lambda);
SELECT 'plan=0';
SELECT arrayMap(lx -> __aliasMarker(lx, lx), [x]) AS arr
FROM t_dist_04278_lambda ORDER BY x
SETTINGS enable_analyzer = 1, serialize_query_plan = 0, prefer_localhost_replica = 0;
SELECT 'plan=1';
SELECT arrayMap(lx -> __aliasMarker(lx, lx), [x]) AS arr
FROM t_dist_04278_lambda ORDER BY x
SETTINGS enable_analyzer = 1, serialize_query_plan = 1, prefer_localhost_replica = 0;
DROP TABLE t_dist_04278_lambda;
DROP TABLE t_local_04278_lambda;
EOF

echo "---- arg2 materialized as action-node-name across mergeable-state stages ----"
# Load-bearing assertion: the planner's `tryExtractAliasMarkerIdFromSecondArgument`
# uses arg2 as the action-node-name, so `sum(__aliasMarker(<arg1>, 'foo'))` becomes
# `sum(foo)` in the EXPLAIN header at the mergeable-state stages where intermediate
# headers are visible. We grep for `sum(foo)` rather than the literal arg1
# expression to stay immune to expression-printer changes.
#
# Stages where the marker is observable on the EXPLAIN header:
#   - with_mergeable_state:                              sum(foo) AggregateFunction(...)
#   - with_mergeable_state_after_aggregation:            sum(foo) Int64
#   - with_mergeable_state_after_aggregation_and_limit:  sum(foo) Int64 (alongside group-by expr)
# At `fetch_columns` the header is the source column; at `complete` the header is
# the user-facing alias `x`. Both are uninformative for this assertion, so we skip them.
for stage in with_mergeable_state with_mergeable_state_after_aggregation with_mergeable_state_after_aggregation_and_limit; do
  header=$($CLICKHOUSE_CLIENT --enable_analyzer=1 --query_kind secondary_query --stage $stage --query \
    "EXPLAIN header=1 SELECT sum(__aliasMarker(number*2-3,'foo')) AS x FROM numbers(10) GROUP BY intDiv(number,10) AS y ORDER BY y LIMIT 10" \
    2>&1)
  if echo "$header" | grep -qE 'sum\(foo\)'; then
    echo "$stage: arg2_materialized"
  else
    echo "$stage: arg2_NOT_materialized"
  fi
done

echo "---- non-analyzer path rejects __aliasMarker ----"
err=$($CLICKHOUSE_CLIENT --enable_analyzer=0 --query "SELECT __aliasMarker(1, 'x')" 2>&1)
if grep -q "Function __aliasMarker is internal and supported only with the analyzer" <<<"$err"; then
  echo "expected_rejection"
else
  echo "unexpected_output: $err"
fi

echo "---- enable_alias_marker=0 disables marker injection ----"
$CLICKHOUSE_CLIENT --multiquery <<'EOF'
DROP TABLE IF EXISTS t_local_04278_swap;
DROP TABLE IF EXISTS t_dist_04278_swap;
CREATE TABLE t_local_04278_swap
(
  dt DateTime,
  f UInt8,
  flag_zero Bool ALIAS toBool(bitTest(f, 0)),
  flag_one Bool ALIAS toBool(bitTest(f, 1))
)
ENGINE = MergeTree ORDER BY dt;
INSERT INTO t_local_04278_swap VALUES ('2024-01-01 00:00:00', 1);
CREATE TABLE t_dist_04278_swap AS t_local_04278_swap
ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), t_local_04278_swap, rand());
EOF
echo "marker_on:"
$CLICKHOUSE_CLIENT --query "SELECT flag_zero, flag_one, bitTest(f, 0) AS x FROM t_dist_04278_swap ORDER BY dt DESC LIMIT 1 SETTINGS enable_alias_marker = 1, enable_analyzer = 1, prefer_localhost_replica = 0"
echo "marker_off:"
# BUG: with enable_alias_marker=0 the inlined ALIAS columns get swapped on the
# remote header (initiator's planner uses Position-mode reconciliation against
# a reordered shard header). The swapped output below is the documented escape-hatch
# behaviour - do NOT "fix" by aligning with marker_on unless the underlying
# Position-mode reconciliation is replaced.
$CLICKHOUSE_CLIENT --query "SELECT flag_zero, flag_one, bitTest(f, 0) AS x FROM t_dist_04278_swap ORDER BY dt DESC LIMIT 1 SETTINGS enable_alias_marker = 0, enable_analyzer = 1, prefer_localhost_replica = 0" 2>&1 | head -1
$CLICKHOUSE_CLIENT --query "DROP TABLE t_dist_04278_swap"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_local_04278_swap"
