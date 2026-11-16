#!/usr/bin/env bash

# Automatic parallel replicas asks whether parallel replicas can read anything for a query before
# building the candidate plan it would cost them, and answers from the storage the catalog holds for
# each table. `CREATE TABLE ... AS view(...)` is attached as a `StorageTableFunctionProxy`, which
# answers `isView() == false` although reading it plans the view's body - and that body is read with
# parallel replicas. `AutomaticParallelReplicasProbePlansBuilt` counts the candidate plans built, so
# it must be non-zero here.
#
# Spelled as a shell test because the view's body is stored as written and resolved later, when the
# proxy materializes, in a context without this test's database: the table has to be qualified, and a
# query parameter in that position would be stored rather than substituted.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE t_05259 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_05259 SELECT number, number % 100 FROM numbers(10000);
CREATE TABLE t_05259_proxy AS view(SELECT a, b FROM ${CLICKHOUSE_DATABASE}.t_05259);
"

# `sum` rather than `count` so that the query reads a column, and
# `automatic_parallel_replicas_min_bytes_per_replica = 0` so that the byte pre-gate does not reject a
# read this small before the eligibility check is reached. Without a local plan no candidate plan is
# built at all, and the plan-based implementation decides eligibility from the plan rather than from
# the query tree; either would make the count vacuous.
${CLICKHOUSE_CLIENT} --query "
SELECT sum(b) FROM t_05259_proxy FORMAT Null
SETTINGS enable_analyzer = 1,
         enable_parallel_replicas = 1,
         cluster_for_parallel_replicas = 'parallel_replicas',
         max_parallel_replicas = 3,
         automatic_parallel_replicas_mode = 1,
         parallel_replicas_for_non_replicated_merge_tree = 1,
         automatic_parallel_replicas_min_bytes_per_replica = 0,
         parallel_replicas_local_plan = 1,
         parallel_replicas_plan_based = 0,
         parallel_replicas_allow_view_over_mergetree = 0,
         log_comment = 'autopr_gate_05259_proxy_over_view';
"

${CLICKHOUSE_CLIENT} --query "
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['AutomaticParallelReplicasProbePlansBuilt'] > 0 AS candidate_plan_built
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = 'autopr_gate_05259_proxy_over_view';
"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE t_05259_proxy;
DROP TABLE t_05259;
"
