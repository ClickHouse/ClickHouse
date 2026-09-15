#!/usr/bin/env bash
# Tags: distributed
# A `SQL SECURITY NONE` view over a `Distributed` table is an optimization barrier even when its body
# is a plain projection: a shard resolves the `Distributed` table to a table of its own and runs the
# shipped query as the cluster's user, so a row policy on the shard-local table hides rows from the
# caller while nothing on the initiator can see that policy. `optimize_trivial_view_pushdown_to_distributed`
# would replace the view with its inner query and ship the invoker's predicate to the shards, where its
# index analysis prunes granules by the values of the hidden rows and `read_rows` tells the invoker
# whether such a row exists. The rewrite must be declined; the invoker's predicate stays on the
# initiator, above the view's read.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user05220_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} <<EOF2
-- \`key\` is the sort key, so it is what the invoker's predicate would prune on. Enough rows for
-- several granules, so that pruning is visible in \`read_rows\` at all.
CREATE TABLE $db.t05220_local (key UInt64, hidden UInt8)
ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 1024;
INSERT INTO $db.t05220_local SELECT number, number = 99999 FROM numbers(100000);

CREATE TABLE $db.t05220_dist AS $db.t05220_local
ENGINE = Distributed(test_cluster_two_shards, '$db', t05220_local);

-- The policy lives on the shard-local table and applies to whichever user the cluster connects as.
CREATE ROW POLICY p05220 ON $db.t05220_local FOR SELECT USING hidden = 0 TO ALL;

-- The view body hides nothing: all the row hiding happens on the shards.
CREATE VIEW $db.v05220 SQL SECURITY NONE AS SELECT key FROM $db.t05220_dist;

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT SELECT ON $db.v05220 TO $user;
EOF2

common_settings="
    SET enable_analyzer = 1;
    SET explain_query_plan_default = 'legacy';
    SET enable_parallel_replicas = 0;
    SET prefer_localhost_replica = 0;
    SET serialize_query_plan = 0;
    SET optimize_trivial_view_pushdown_to_distributed = 1;
"

echo "=== the shard-local policy hides the row on both shards ==="
${CLICKHOUSE_CLIENT} --user "$user" --query "
    ${common_settings}
    SELECT count() FROM $db.v05220;
"

echo "=== pushdown declined, the view stays a subquery ==="
${CLICKHOUSE_CLIENT} --query "
    ${common_settings}
    SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS view_is_a_barrier
    FROM (EXPLAIN SELECT key FROM $db.v05220 WHERE key = 99999);
"

# `99999` exists on the shards but is hidden by their row policy; `500000` exists nowhere. With the
# barrier the two must read the same number of rows, so the invoker learns nothing about the hidden
# row. The comparison is an exact equality of `read_rows` between two runs, so the per-query random
# read-path injections the test harness enables must be pinned off, and a single thread keeps the
# read pool deterministic.
probe() {
    local query_id="probe_${CLICKHOUSE_DATABASE}_$1"
    ${CLICKHOUSE_CLIENT} --user "$user" --query_id "$query_id" \
        --enable_analyzer 1 \
        --enable_parallel_replicas 0 \
        --prefer_localhost_replica 0 \
        --serialize_query_plan 0 \
        --optimize_trivial_view_pushdown_to_distributed 1 \
        --max_threads 1 \
        --merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability 0 \
        --page_cache_inject_eviction 0 \
        --query "SELECT count() FROM $db.v05220 WHERE key = $1" > /dev/null
    echo "$query_id"
}

echo "=== reading the view costs the same whether or not the hidden row matches ==="
hidden_id=$(probe 99999)
absent_id=$(probe 500000)

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
# `count() = 2` guards against the comparison passing vacuously on an empty match.
${CLICKHOUSE_CLIENT} --query "
    SELECT multiIf(
        count() != 2, 'MISSING',
        anyIf(read_rows, query_id = '$hidden_id') = anyIf(read_rows, query_id = '$absent_id'),
        'same', 'DISCLOSED')
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND query_id IN ('$hidden_id', '$absent_id') AND type = 'QueryFinish'"

${CLICKHOUSE_CLIENT} --query "
    DROP USER $user;
    DROP ROW POLICY p05220 ON $db.t05220_local;
"
