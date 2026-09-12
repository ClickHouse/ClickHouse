#!/usr/bin/env bash
# Regression test for the AST-fuzzer-found "Not-ready Set is passed as the
# second argument for function 'in'" logical error in
# `OptimizeShardingKeyRewriteIn` (STID 0250-4e52).
#
# When the sharding key of a `Distributed` table contains an `IN`/subquery
# (a set), the sharding-key `ExpressionActions` is built standalone from the
# sharding-key AST, so that set is never populated during planning. The
# shard-pruning code executes this expression on constant values to prune
# shards, which hits `FunctionIn` with an unbuilt set and throws
# "Not-ready Set" (a LOGICAL_ERROR: server abort in debug/sanitizer builds,
# a thrown exception in release builds).
#
# The fix skips the optimization only when the sharding key contains such an
# UNREADY subquery-backed set and queries all shards instead (the same safe
# fallback used for non-deterministic sharding keys). It does NOT bail for
# already materialized tuple/storage sets, so shard pruning still applies to
# safe constant-set keys. Both the analyzer rewrite path and the old-analyzer
# `StorageDistributed::skipUnusedShards` path are guarded.
#
# The success case is made OBSERVABLE rather than accepting any non-abort exit:
#   * Unready-set cases: the fallback must query ALL shards, so both configured
#     hosts have to appear in stderr. Merely getting past planning is not enough:
#     on the analyzer path an unfixed build does not abort, it reads the unbuilt
#     set as a constant and prunes to a single shard, silently dropping the rows
#     on the other one. Requiring both hosts catches that, and the abort/throw
#     shapes are still rejected explicitly.
#   * Ready-tuple cases: shard pruning must be PRESERVED, so the query is
#     directed at values that map to a single shard (shard 1, 192.0.2.2). With
#     pruning it contacts only 192.0.2.2; a regression to always-fallback would
#     also try shard 0 (192.0.2.1). The shards use RFC5737 TEST-NET addresses,
#     unreachable in every environment (including the single-node Fast test box
#     where 127.0.0.1:9000 is a live server), so the shard actually contacted
#     shows up as the connection-error host.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLUSTER_CONFIG="${CLICKHOUSE_TMP}/04545_cluster.xml"
cat > "${CLUSTER_CONFIG}" <<'EOF'
<clickhouse>
    <remote_servers>
        <test_04545_two_shards>
            <shard>
                <replica>
                    <host>192.0.2.1</host>
                    <port>9000</port>
                </replica>
            </shard>
            <shard>
                <replica>
                    <host>192.0.2.2</host>
                    <port>9000</port>
                </replica>
            </shard>
        </test_04545_two_shards>
    </remote_servers>
</clickhouse>
EOF

# Low failover connect timeout so the unreachable TEST-NET shards fail fast.
COMMON_SETTINGS="prefer_localhost_replica = 0, optimize_skip_unused_shards = 1, optimize_skip_unused_shards_rewrite_in = 1, allow_nondeterministic_optimize_skip_unused_shards = 1, connect_timeout_with_failover_ms = 300"

# Unready subquery-backed set: the fix must fall back to querying ALL shards.
# The bug leaks in three shapes, all of which must be caught:
#   * release build, old analyzer: "Not-ready Set" is thrown and printed to stderr;
#   * debug/sanitizer build, old analyzer: it aborts (exit 134);
#   * any build, analyzer: no error at all. The unbuilt set is read as a constant,
#     the key is const-folded to one value and the query is pruned to a single
#     shard, so rows on the other shard are silently dropped.
# `skip_unavailable_shards = 1` keeps the first unreachable shard from aborting the
# query, so every shard the planner selected reports its own connection failure and
# the whole selected set is visible in stderr. `--send_logs_level=information` is
# what surfaces those failures. Requiring BOTH hosts (rather than any one host) is
# what makes the analyzer shape observable. $1 = analyzer flag, $2 = sharding key.
run_unready()
{
    local analyzer="$1" key="$2" err rc
    err=$(${CLICKHOUSE_LOCAL} --config-file="${CLUSTER_CONFIG}" --send_logs_level=information --query "
        CREATE TABLE dist_04545 AS system.one
            ENGINE = Distributed(test_04545_two_shards, system, one, ${key});
        SET allow_experimental_analyzer = ${analyzer}, ${COMMON_SETTINGS}, skip_unavailable_shards = 1;
        SELECT count() FROM dist_04545 WHERE dummy IN (1, 3);
    " 2>&1 >/dev/null)
    rc=$?

    if [ "${rc}" = "134" ] || echo "${err}" | grep -q "Not-ready Set"; then
        echo "NOT-READY-SET-LEAK"
    elif echo "${err}" | grep -q "192\.0\.2\.1" && echo "${err}" | grep -q "192\.0\.2\.2"; then
        echo "ALL-SHARDS"
    else
        echo "PARTIAL-SHARDS"
    fi
}

# Ready tuple set: `0 IN (1, 2)` is a materialized constant set, so the sharding
# key reduces to `bitAnd(dummy, 1)` = dummy % 2. Shard pruning must survive.
# `dummy IN (1, 3)` both map to shard 1 (192.0.2.2); with pruning only that
# shard is contacted, without pruning shard 0 (192.0.2.1) is contacted too.
# $1 = analyzer flag, $2 = sharding key.
run_ready_pruned()
{
    local analyzer="$1" key="$2" err
    err=$(${CLICKHOUSE_LOCAL} --config-file="${CLUSTER_CONFIG}" --send_logs_level=fatal --query "
        CREATE TABLE dist_04545 AS system.one
            ENGINE = Distributed(test_04545_two_shards, system, one, ${key});
        SET allow_experimental_analyzer = ${analyzer}, ${COMMON_SETTINGS};
        SELECT count() FROM dist_04545 WHERE dummy IN (1, 3);
    " 2>&1 >/dev/null)

    if echo "${err}" | grep -q "192.0.2.2" && ! echo "${err}" | grep -q "192.0.2.1"; then
        echo "PRUNED-TO-SHARD1"
    else
        echo "NOT-PRUNED"
    fi
}

# Unready subquery-backed set: must fall back, not abort. Analyzer + old analyzer.
run_unready 1 'bitAnd(dummy + (0 IN (SELECT 1)), 1)'
run_unready 0 'bitAnd(dummy + (0 IN (SELECT 1)), 1)'
# Same unready set, but written inside a lambda body. A higher-order key keeps its
# body in a separate nested DAG, yet the set itself does not live there: the
# `ColumnConst(ColumnSet)` is added by `ScopeStack::addColumn`, which always puts it
# in the outermost DAG and only projects an input into the nested scopes. So the
# guard sees it without walking lambda bodies. Pin that, because the shape is
# reachable: without the guard these abort on the old-analyzer path.
run_unready 1 'arrayExists(x -> x IN (SELECT 1), [dummy])'
run_unready 0 'arrayExists(x -> x IN (SELECT 1), [dummy])'
# Nested lambdas: the set stays in the outermost DAG at any nesting depth.
run_unready 1 'arrayExists(x -> arrayExists(y -> y IN (SELECT 1), [x]), [dummy])'
run_unready 0 'arrayExists(x -> arrayExists(y -> y IN (SELECT 1), [x]), [dummy])'
# Ready tuple set: safe, shard pruning must be preserved. Analyzer + old analyzer.
run_ready_pruned 1 'bitAnd(dummy + (0 IN (1, 2)), 1)'
run_ready_pruned 0 'bitAnd(dummy + (0 IN (1, 2)), 1)'
# Ready tuple set written inside a lambda body: the guard reports only unbuilt sets,
# so pruning is still preserved here. Analyzer + old analyzer.
run_ready_pruned 1 'bitAnd(dummy + arrayExists(x -> x IN (1, 2), [0]), 1)'
run_ready_pruned 0 'bitAnd(dummy + arrayExists(x -> x IN (1, 2), [0]), 1)'

rm -f "${CLUSTER_CONFIG}"
