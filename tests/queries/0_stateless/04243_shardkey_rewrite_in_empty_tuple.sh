#!/usr/bin/env bash
# Regression test for the AST-fuzzer-found exception in
# `OptimizeShardingKeyRewriteIn::enterImpl`: when the analyzer-side rewriter
# visited an empty constant tuple `IN tuple()` against the sharding-key column
# of a `Distributed` table, it called `Tuple::back()` on an empty `std::vector`,
# aborting under libc++ hardening with
# "back() called on an empty vector".
#
# The trigger needs the rewriter to actually run on the empty tuple, which
# requires `optimize_skip_unused_shards` to populate `optimized_cluster` (so
# `shards > 1`). An empty `IN ()` on the sharding key column in `WHERE` fully
# prunes all shards and bypasses the rewriter, so the test combines:
#   * a non-empty `WHERE` `IN` on the sharding key (keeps shards alive), and
#   * an empty `tuple()` `IN` in `GROUP BY` (visited by the rewriter and
#     previously crashing).
#
# We run the bug-triggering scenario inside a `clickhouse-local` subprocess so
# the abort stays contained. The bugfix-validation framework needs an output
# diff `FAIL` on master HEAD (which it then inverts to `OK`); a server-side
# crash is classified as `SERVER_DIED` and is not invertible.
#
# `clickhouse-local` has no clusters by default, so we feed it a tiny config
# with a two-shard `remote_servers` definition. The rewriter only needs the
# cluster to exist with `shards > 1`; it does not actually connect to the
# shards during analysis.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLUSTER_CONFIG="${CLICKHOUSE_TMP}/04243_cluster.xml"
cat > "${CLUSTER_CONFIG}" <<'EOF'
<clickhouse>
    <remote_servers>
        <test_04243_two_shards>
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
        </test_04243_two_shards>
    </remote_servers>
</clickhouse>
EOF

# The rewriter runs during query planning, so `EXPLAIN` is enough to trip it on
# master HEAD without us having to actually connect to the fake shards. With
# the fix, `EXPLAIN` completes cleanly (exit 0). Without the fix, the planner
# aborts inside `OptimizeShardingKeyRewriteIn::enterImpl` on the empty
# `Tuple::back()` (exit 134 under libc++ hardening).
#
# Suppress stack-trace / abort spam from the contained crash on master HEAD.
if ${CLICKHOUSE_LOCAL} --config-file="${CLUSTER_CONFIG}" --send_logs_level=fatal --query "
    CREATE TABLE dist_04243 AS system.one
        ENGINE = Distributed(test_04243_two_shards, system, one, intHash64(dummy));

    SET prefer_localhost_replica = 0;
    SET optimize_skip_unused_shards = 1;
    SET optimize_skip_unused_shards_rewrite_in = 1;

    EXPLAIN SELECT count() FROM dist_04243
    WHERE dummy IN (0, 2)
    GROUP BY (dummy IN tuple());
" > /dev/null 2>&1
then
    echo "OK"
else
    echo "BUG"
fi

rm -f "${CLUSTER_CONFIG}"
