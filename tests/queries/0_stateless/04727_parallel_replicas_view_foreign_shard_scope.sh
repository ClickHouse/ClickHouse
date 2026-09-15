#!/usr/bin/env bash
# Tags: shard

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `_shard_num` is shipped by the initiator of a distributed query, while the view's own SETTINGS name a
# different `cluster_for_parallel_replicas`. A shard number produced for one cluster does not index
# another: out of range it threw "Shard number is greater than shard count", in range it silently read
# the wrong shard.

# Every arm pins the settings that select the code path under test:
#   prefer_localhost_replica = 0     -- a local replica ships no `_shard_num` over the wire at all
#                                       (the last arm below is the control for this)
#   parallel_replicas_plan_based = 0 -- the plan-based path builds locally and never applies a shard scope
#   enable_analyzer, parallel_replicas_only_with_analyzer -- parallel replicas are off entirely unless
#       the two agree, so each arm pins the analyzer it measures instead of inheriting the profile
# `serialize_query_plan` needs no pin here and was measured, not assumed: it does suppress parallel
# replicas for a read of a plain table (see 02947/03562, which pin it for that reason), but each view
# below carries `enable_parallel_replicas` in its own SETTINGS, which the shard re-applies, so the
# matching arm's oracle still reads 1 under the `distributed plan` profile.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE t_04727 (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_04727 SELECT number FROM numbers(100);

-- The view's parallel-replicas cluster has ONE shard, so a shipped shard_num=2 is out of range.
CREATE VIEW v_out_of_range_04727 AS SELECT a FROM t_04727
  SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
           parallel_replicas_for_non_replicated_merge_tree = 1,
           automatic_parallel_replicas_mode = 0,
           cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- ... and here it has TWO, so shard_num=2 is in range: the bounds check passes and the read is
-- silently scoped to an unrelated shard of a cluster the outer query never addressed.
CREATE VIEW v_in_range_04727 AS SELECT a FROM t_04727
  SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
           parallel_replicas_for_non_replicated_merge_tree = 1,
           automatic_parallel_replicas_mode = 0,
           cluster_for_parallel_replicas = 'test_cluster_two_shard_three_replicas_localhost';

-- Without the analyzer the storage decides the processing stage, so the work the view's own plan is
-- expected to do must actually happen there: a WHERE, and an aggregate whose partial state the
-- initiator merges.
CREATE VIEW v_filter_04727 AS SELECT a FROM t_04727 WHERE a >= 50
  SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
           parallel_replicas_for_non_replicated_merge_tree = 1,
           automatic_parallel_replicas_mode = 0,
           cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

CREATE VIEW v_aggregate_04727 AS SELECT sum(a) AS s FROM t_04727 WHERE a >= 50
  SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
           parallel_replicas_for_non_replicated_merge_tree = 1,
           automatic_parallel_replicas_mode = 0,
           cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
"

# Declining the scope has to be equivalent to `enable_parallel_replicas = 0`, and without the analyzer
# that is not settled by the read alone: `ExpressionAnalyzer::isRemoteStorage` consults the raw setting,
# so `GlobalSubqueriesVisitor` rewrites an ordinary `IN (subquery)` into the external-table (`GLOBAL`)
# path before the storage gets to decline the foreign scope. The two views below are the same query and
# differ only in that setting, so the pair measures that rewrite rather than assuming it is harmless.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE u_04727 (b UInt64) ENGINE = MergeTree ORDER BY b;
INSERT INTO u_04727 SELECT number FROM numbers(50, 50);

CREATE VIEW v_in_subquery_04727 AS SELECT a FROM t_04727 WHERE a IN (SELECT b FROM u_04727)
  SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
           parallel_replicas_for_non_replicated_merge_tree = 1,
           automatic_parallel_replicas_mode = 0,
           cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

CREATE VIEW v_in_subquery_off_04727 AS SELECT a FROM t_04727 WHERE a IN (SELECT b FROM u_04727)
  SETTINGS enable_parallel_replicas = 0;
"

# Reports whether parallel replicas were actually used by the arm tagged with $1, so that an arm cannot
# pass by silently falling back to a plain local read. Same shape as
# 02875_parallel_replicas_cluster_all_replicas.
parallel_replicas_used() {
    $CLICKHOUSE_CLIENT -q "
    SELECT countIf(ProfileEvents['ParallelReplicasQueryCount'] > 0) > 0 FROM system.query_log
    WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
      AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish'
          AND event_date >= yesterday() AND log_comment = '$1')
    SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0"
}

# Each shard of test_cluster_two_shards_localhost points at this same server and table, so the value is
# the view's sum counted twice on every arm; it is `parallel_replicas_used` that tells the arms apart.
echo '-- out-of-range foreign shard scope: declined, so no abort'
$CLICKHOUSE_CLIENT -q "
SELECT sum(a) FROM cluster('test_cluster_two_shards_localhost', currentDatabase(), v_out_of_range_04727)
SETTINGS prefer_localhost_replica = 0, parallel_replicas_plan_based = 0,
         enable_analyzer = 1, parallel_replicas_only_with_analyzer = 0,
         log_comment = '04727_out_of_range_${CLICKHOUSE_DATABASE}';
"

echo '-- in-range foreign shard scope: declined, so the wrong shard is never read'
$CLICKHOUSE_CLIENT -q "
SELECT sum(a) FROM cluster('test_cluster_two_shards_localhost', currentDatabase(), v_in_range_04727)
SETTINGS prefer_localhost_replica = 0, parallel_replicas_plan_based = 0,
         enable_analyzer = 1, parallel_replicas_only_with_analyzer = 0,
         log_comment = '04727_in_range_${CLICKHOUSE_DATABASE}';
"

echo '-- matching shard scope: still honoured, i.e. the feature is declined and not disabled'
$CLICKHOUSE_CLIENT -q "
SELECT sum(a) FROM cluster('test_cluster_two_shard_three_replicas_localhost', currentDatabase(), v_in_range_04727)
SETTINGS prefer_localhost_replica = 0, parallel_replicas_plan_based = 0,
         enable_analyzer = 1, parallel_replicas_only_with_analyzer = 0,
         log_comment = '04727_matching_${CLICKHOUSE_DATABASE}';
"

# `clusterAllReplicas` turns every replica into a shard of its own, so the cluster it reads through has 6
# shards numbered 1..6 while carrying the same NAME as the 2-shard config cluster the view names. Comparing
# names authenticated a derived shard number against the config cluster: 3..6 aborted, 1..2 read a wrong
# shard. The scope must therefore identify the shard NUMBERING, not the cluster name.
echo '-- renumbered derived cluster: declined, so no abort and no wrong shard'
$CLICKHOUSE_CLIENT -q "
SELECT sum(a) FROM clusterAllReplicas('test_cluster_two_shard_three_replicas_localhost', currentDatabase(), v_in_range_04727)
SETTINGS prefer_localhost_replica = 0, parallel_replicas_plan_based = 0,
         enable_analyzer = 1, parallel_replicas_only_with_analyzer = 0,
         log_comment = '04727_derived_${CLICKHOUSE_DATABASE}';
"

# Without the analyzer the storage decides both the processing stage and whether to read through
# parallel replicas, and the two decisions must agree. A stage of WithMergeableState above a plan that
# was in fact built locally makes the initiator treat raw rows as partial aggregate states and skip the
# first-stage work: the filter below is then not applied, and the aggregate below is not found at all.
echo '-- out-of-range foreign shard scope, old analyzer: the view filter is applied'
$CLICKHOUSE_CLIENT -q "
SELECT sum(a) FROM cluster('test_cluster_two_shards_localhost', currentDatabase(), v_filter_04727)
SETTINGS prefer_localhost_replica = 0, parallel_replicas_plan_based = 0,
         enable_analyzer = 0, parallel_replicas_only_with_analyzer = 0,
         log_comment = '04727_old_analyzer_filter_${CLICKHOUSE_DATABASE}';
"

echo '-- out-of-range foreign shard scope, old analyzer: the view aggregate is computed'
$CLICKHOUSE_CLIENT -q "
SELECT sum(s) FROM cluster('test_cluster_two_shards_localhost', currentDatabase(), v_aggregate_04727)
SETTINGS prefer_localhost_replica = 0, parallel_replicas_plan_based = 0,
         enable_analyzer = 0, parallel_replicas_only_with_analyzer = 0,
         log_comment = '04727_old_analyzer_aggregate_${CLICKHOUSE_DATABASE}';
"

echo '-- out-of-range foreign shard scope, old analyzer: IN (subquery) reads shard-local rows'
$CLICKHOUSE_CLIENT -q "
SELECT sum(a) FROM cluster('test_cluster_two_shards_localhost', currentDatabase(), v_in_subquery_04727)
SETTINGS prefer_localhost_replica = 0, parallel_replicas_plan_based = 0,
         enable_analyzer = 0, parallel_replicas_only_with_analyzer = 0,
         log_comment = '04727_old_analyzer_in_subquery_${CLICKHOUSE_DATABASE}';
"

echo '-- ... and the same query with enable_parallel_replicas = 0 gives the same answer'
$CLICKHOUSE_CLIENT -q "
SELECT sum(a) FROM cluster('test_cluster_two_shards_localhost', currentDatabase(), v_in_subquery_off_04727)
SETTINGS prefer_localhost_replica = 0, parallel_replicas_plan_based = 0,
         enable_analyzer = 0, parallel_replicas_only_with_analyzer = 0,
         log_comment = '04727_old_analyzer_in_subquery_off_${CLICKHOUSE_DATABASE}';
"

# Anti-vacuity control for the `prefer_localhost_replica = 0` pin every arm above carries: at 1 the shard
# is served locally, so no scope is shipped and none can be applied -- yet parallel replicas still run.
echo '-- local shard, no foreign scope shipped: read is correct'
$CLICKHOUSE_CLIENT -q "
SELECT sum(a) FROM cluster('test_cluster_two_shards_localhost', currentDatabase(), v_out_of_range_04727)
SETTINGS prefer_localhost_replica = 1, parallel_replicas_plan_based = 0,
         enable_analyzer = 1, parallel_replicas_only_with_analyzer = 0,
         log_comment = '04727_local_shard_${CLICKHOUSE_DATABASE}';
"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

echo '-- parallel replicas used? out_of_range / in_range / matching / derived / old analyzer filter, aggregate, IN (subquery), IN (subquery) with the feature off / local shard'
parallel_replicas_used "04727_out_of_range_${CLICKHOUSE_DATABASE}"
parallel_replicas_used "04727_in_range_${CLICKHOUSE_DATABASE}"
parallel_replicas_used "04727_matching_${CLICKHOUSE_DATABASE}"
parallel_replicas_used "04727_derived_${CLICKHOUSE_DATABASE}"
parallel_replicas_used "04727_old_analyzer_filter_${CLICKHOUSE_DATABASE}"
parallel_replicas_used "04727_old_analyzer_aggregate_${CLICKHOUSE_DATABASE}"
parallel_replicas_used "04727_old_analyzer_in_subquery_${CLICKHOUSE_DATABASE}"
parallel_replicas_used "04727_old_analyzer_in_subquery_off_${CLICKHOUSE_DATABASE}"
parallel_replicas_used "04727_local_shard_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
DROP VIEW v_in_subquery_off_04727; DROP VIEW v_in_subquery_04727;
DROP VIEW v_aggregate_04727; DROP VIEW v_filter_04727;
DROP VIEW v_in_range_04727; DROP VIEW v_out_of_range_04727;
DROP TABLE u_04727; DROP TABLE t_04727;"
