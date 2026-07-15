#!/usr/bin/env bash
# Regression test for the AST-fuzzer-found "Not-ready Set is passed as the
# second argument for function 'in'" logical error in
# `OptimizeShardingKeyRewriteIn` (STID 0250-4e52).
#
# When the sharding key of a `Distributed` table contains an `IN`/subquery
# (a set), the sharding-key `ExpressionActions` is built standalone from the
# sharding-key AST, so that set is never populated during planning. The
# shard-pruning code executes this expression on constant values to prune
# shards, which hits `FunctionIn` with an unbuilt set and aborts
# (exit 134 in debug/sanitizer builds).
#
# The fix skips the optimization when the sharding key contains such an
# UNREADY set and queries all shards instead (the same safe fallback used
# for non-deterministic sharding keys). It does NOT bail for already
# materialized tuple/storage sets, so shard pruning still applies to safe
# constant-set keys. Both the analyzer rewrite path and the old-analyzer
# `StorageDistributed::skipUnusedShards` path are guarded.
#
# Modelled on 04243_shardkey_rewrite_in_empty_tuple.sh: run inside a
# `clickhouse-local` subprocess with a two-shard cluster so the abort stays
# contained. The analyzer path is tripped during planning by `EXPLAIN`; the
# old-analyzer path only runs shard skipping for a real `SELECT`, so those
# subtests execute the query (the fake shards are unreachable, so a healthy
# run ends in a network error, exit != 134 = OK, exit 134 = crash = BUG).

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
                    <host>127.0.0.1</host>
                    <port>9000</port>
                </replica>
            </shard>
            <shard>
                <replica>
                    <host>127.0.0.2</host>
                    <port>9000</port>
                </replica>
            </shard>
        </test_04545_two_shards>
    </remote_servers>
</clickhouse>
EOF

COMMON_SETTINGS="prefer_localhost_replica = 0, optimize_skip_unused_shards = 1, optimize_skip_unused_shards_rewrite_in = 1, allow_nondeterministic_optimize_skip_unused_shards = 1"

# $1 = analyzer flag, $2 = sharding key expression, $3 = "explain" | "select".
# Prints OK unless the planner aborts (exit 134).
run_case()
{
    local analyzer="$1" key="$2" mode="$3" query
    if [ "$mode" = "explain" ]; then
        query="EXPLAIN SELECT count() FROM dist_04545 WHERE dummy IN (0, 1)"
    else
        query="SELECT count() FROM dist_04545 WHERE dummy IN (0, 1)"
    fi

    ${CLICKHOUSE_LOCAL} --config-file="${CLUSTER_CONFIG}" --send_logs_level=fatal --query "
        CREATE TABLE dist_04545 AS system.one
            ENGINE = Distributed(test_04545_two_shards, system, one, ${key});
        SET allow_experimental_analyzer = ${analyzer}, ${COMMON_SETTINGS};
        ${query};
    " > /dev/null 2>&1

    # 134 = SIGABRT (the "Not-ready Set" logical error). Any other exit (0, or a
    # network error from the unreachable fake shards) means planning succeeded.
    if [ "$?" = "134" ]; then
        echo "BUG"
    else
        echo "OK"
    fi
}

# Unready subquery-backed set: must fall back, not abort. Analyzer + old analyzer.
run_case 1 'bitAnd(dummy + (0 IN (SELECT 1)), 1)' explain
run_case 0 'bitAnd(dummy + (0 IN (SELECT 1)), 1)' select
# Ready tuple set: safe, pruning still applies, must not abort. Analyzer + old analyzer.
run_case 1 'bitAnd(dummy + (0 IN (1, 2)), 1)' explain
run_case 0 'bitAnd(dummy + (0 IN (1, 2)), 1)' select

rm -f "${CLUSTER_CONFIG}"
