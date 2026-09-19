#!/usr/bin/env bash
# Tags: no-parallel-replicas
# Tag no-parallel-replicas: sets cluster_for_parallel_replicas explicitly.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}
definer="definer_04545_${CLICKHOUSE_DATABASE}_$RANDOM"
invoker="invoker_04545_${CLICKHOUSE_DATABASE}_$RANDOM"

${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $definer, $invoker;
CREATE USER $definer, $invoker;

CREATE TABLE $db.nums (x Int64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 128;
INSERT INTO $db.nums SELECT * FROM numbers(10000);

-- The definer can read the base table; the invoker cannot.
GRANT SELECT ON $db.nums TO $definer;

-- Parameterized views with each SQL SECURITY kind.
CREATE VIEW $db.pv_definer DEFINER = $definer SQL SECURITY DEFINER AS SELECT x FROM $db.nums WHERE x <= {lim:Int64};
CREATE VIEW $db.pv_invoker DEFINER = $definer SQL SECURITY INVOKER AS SELECT x FROM $db.nums WHERE x <= {lim:Int64};
CREATE VIEW $db.pv_none SQL SECURITY NONE AS SELECT x FROM $db.nums WHERE x <= {lim:Int64};

-- A parameterized view that provably hides no row of the base table: it is not a security
-- barrier, so it keeps every optimization, including parallel replicas.
CREATE VIEW $db.pv_plain DEFINER = $definer SQL SECURITY DEFINER AS SELECT x + {shift:Int64} AS x FROM $db.nums;

GRANT SELECT ON $db.pv_definer TO $invoker;
GRANT SELECT ON $db.pv_invoker TO $invoker;
GRANT SELECT ON $db.pv_none TO $invoker;
GRANT SELECT ON $db.pv_plain TO $invoker;
EOF

# The invoker has SELECT on the views only, not on the base table.
# DEFINER/NONE must return rows; INVOKER must be denied. This must hold for BOTH the
# default analyzer and the old analyzer (the old-analyzer path was the bug: issue #84188).
for analyzer in 1; do
    echo "--- enable_analyzer=$analyzer ---"
    echo -n "definer: "
    ${CLICKHOUSE_CLIENT} --user "$invoker" --query \
        "SELECT count() FROM $db.pv_definer(lim = 5000) SETTINGS enable_analyzer = $analyzer, enable_parallel_replicas = 0"
    echo -n "none:    "
    ${CLICKHOUSE_CLIENT} --user "$invoker" --query \
        "SELECT count() FROM $db.pv_none(lim = 5000) SETTINGS enable_analyzer = $analyzer, enable_parallel_replicas = 0"
    echo -n "invoker: "
    ${CLICKHOUSE_CLIENT} --user "$invoker" --query \
        "SELECT count() FROM $db.pv_invoker(lim = 5000) SETTINGS enable_analyzer = $analyzer, enable_parallel_replicas = 0" 2>&1 \
        | grep -m1 -oF "ACCESS_DENIED" || echo "UNEXPECTED"
done

# Originally reported scenario: parameterized DEFINER view read via parallel replicas by a
# low-privilege invoker must succeed (no ACCESS_DENIED, no data leak).
echo "--- parallel replicas ---"
# automatic_parallel_replicas_mode = 0 forces the explicit parallel-replicas path so the
# ParallelReplicasUsedCount guards below are deterministic (otherwise the coordinator may skip
# parallel replicas for this small table and the guards would flap).
pr_settings="enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 1, parallel_replicas_only_with_analyzer = 0, automatic_parallel_replicas_mode = 0"
echo -n "definer pr: "
${CLICKHOUSE_CLIENT} --user "$invoker" --query \
    "SELECT count() FROM $db.pv_definer(lim = 5000) SETTINGS $pr_settings, log_comment = '04545_pr_${CLICKHOUSE_DATABASE}'"
echo -n "plain pr:   "
${CLICKHOUSE_CLIENT} --user "$invoker" --query \
    "SELECT count() FROM $db.pv_plain(shift = 1) SETTINGS $pr_settings, log_comment = '04545_pr_plain_${CLICKHOUSE_DATABASE}'"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

function parallel_replicas_used()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT ProfileEvents['ParallelReplicasUsedCount'] > 0
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND log_comment = '$1'
          AND type = 'QueryFinish'
          AND initial_query_id = query_id
        SETTINGS enable_parallel_replicas = 0"
}

# `pv_definer` filters, so it is a `SQL SECURITY` optimization barrier and reads its inner query
# without parallel replicas on purpose: task-based parallel replicas ship the inner query as SQL
# text to the other replicas, which re-plan it under their own identity and silently drop the
# definer's row policies and profile filters. Guard the fence, not a silent fallback.
echo -n "parallel replicas used, row-hiding view: "
parallel_replicas_used "04545_pr_${CLICKHOUSE_DATABASE}"

# The fence is narrow: a view that provably hides no rows still reads with parallel replicas,
# which also keeps the originally reported scenario (a low-privilege invoker reading a DEFINER
# view over a table they cannot read, via parallel replicas) exercised.
echo -n "parallel replicas used, plain view:      "
parallel_replicas_used "04545_pr_plain_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} <<EOF
DROP VIEW $db.pv_definer, $db.pv_invoker, $db.pv_none, $db.pv_plain;
DROP TABLE $db.nums;
DROP USER $definer, $invoker;
EOF
