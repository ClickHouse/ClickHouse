#!/usr/bin/env bash
# Regression test for the AST-fuzzer-found "Not-ready Set is passed as the
# second argument for function 'in'" logical error in
# `OptimizeShardingKeyRewriteIn` (STID 0250-4e52).
#
# When the sharding key of a `Distributed` table contains an `IN`/subquery
# (a set), the sharding-key `ExpressionActions` is built standalone from the
# sharding-key AST, so that set is never populated during planning. The
# shard-pruning rewriter executes this expression on constant values to prune
# shards, which hits `FunctionIn` with an unbuilt set and aborts.
#
# The fix skips the rewrite when the sharding key contains a set (all shards
# are queried, the same safe fallback used for non-deterministic sharding
# keys). The rewriter runs during query planning, so `EXPLAIN` is enough to
# trip it on master HEAD without connecting to the fake shards. With the fix
# `EXPLAIN` completes cleanly (exit 0); without it the planner aborts
# (exit 134 in debug/sanitizer builds).
#
# Modelled on 04243_shardkey_rewrite_in_empty_tuple.sh: run inside a
# `clickhouse-local` subprocess with a two-shard cluster so the abort stays
# contained and the bugfix-validation framework sees an invertible output diff.

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

# Sharding key `bitAnd(dummy + (0 IN (SELECT 1)), 1)` contains a subquery-IN.
if ${CLICKHOUSE_LOCAL} --config-file="${CLUSTER_CONFIG}" --send_logs_level=fatal --query "
    CREATE TABLE dist_04545 AS system.one
        ENGINE = Distributed(test_04545_two_shards, system, one, bitAnd(dummy + (0 IN (SELECT 1)), 1));

    SET prefer_localhost_replica = 0;
    SET optimize_skip_unused_shards = 1;
    SET optimize_skip_unused_shards_rewrite_in = 1;
    SET allow_nondeterministic_optimize_skip_unused_shards = 1;

    EXPLAIN SELECT count() FROM dist_04545 WHERE dummy IN (0, 1);
" > /dev/null 2>&1
then
    echo "OK"
else
    echo "BUG"
fi

rm -f "${CLUSTER_CONFIG}"
