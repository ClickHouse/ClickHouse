#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_TRACE=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=trace"}

PARALLEL_REPLICAS_SETTINGS="enable_parallel_replicas=1, max_parallel_replicas=2, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree=1, enable_analyzer=1, parallel_replicas_filter_pushdown=1"

$CLICKHOUSE_CLIENT --query "
drop table if exists t_03733;
drop table if exists v_03733;
create table t_03733(a UInt32, b String) engine=MergeTree order by ();
create view v_03733 as select * from t_03733;
insert into t_03733 select number, toString(number) from numbers(10);
"

# check filter push down for remote
$CLICKHOUSE_CLIENT_TRACE --query "
SET ${PARALLEL_REPLICAS_SETTINGS};
SELECT * FROM v_03733 WHERE a = 0 SETTINGS parallel_replicas_local_plan=0;
" |& grep 'executeQuery' | grep -q 'HAVING' && echo "filter pushed down for remote nodes";
# check filter pushdown for local replica
$CLICKHOUSE_CLIENT --query "
SET ${PARALLEL_REPLICAS_SETTINGS};
select trimLeft(explain) from
  (explain description=0, actions=1 select * from v_03733 where a = 0 settings parallel_replicas_local_plan=1)
where explain ilike '%Prewhere%' limit 1;"

# view inside view
$CLICKHOUSE_CLIENT --query "
drop table if exists vv_03733;
create view vv_03733 as select * from v_03733 order by a desc;
"
# check filter push down for remote
$CLICKHOUSE_CLIENT_TRACE --query "
SET ${PARALLEL_REPLICAS_SETTINGS};
SELECT * FROM vv_03733 WHERE a = 0 SETTINGS parallel_replicas_local_plan=0;
" |& grep 'executeQuery' | grep -q 'HAVING' && echo "filter pushed down for remote nodes";
# check filter pushdown for local replica
$CLICKHOUSE_CLIENT --query "
SET ${PARALLEL_REPLICAS_SETTINGS};
select trimLeft(explain) from
  (explain description=0, actions=1 select * from vv_03733 where a = 0 settings parallel_replicas_local_plan=1)
where explain ilike '%Prewhere%' limit 1;"

# view with column aliases
$CLICKHOUSE_CLIENT --query "
drop table if exists v1_03733;
create view v1_03733 as select a as c, b as d from t_03733;
"
# check filter push down for remote
$CLICKHOUSE_CLIENT_TRACE --query "
SET ${PARALLEL_REPLICAS_SETTINGS};
SELECT * FROM v1_03733 WHERE c = 0 SETTINGS parallel_replicas_local_plan=0;
" |& grep 'executeQuery' | grep -q 'HAVING' && echo "filter pushed down for remote nodes";
# check filter pushdown for local replica
$CLICKHOUSE_CLIENT --query "
SET ${PARALLEL_REPLICAS_SETTINGS};
select trimLeft(explain) from
  (explain description=0, actions=1 select * from v1_03733 where c = 0 settings parallel_replicas_local_plan=1)
where explain ilike '%Prewhere%' limit 1;"

# check filter pushdown can be disabled by setting
$CLICKHOUSE_CLIENT_TRACE --query "
SET ${PARALLEL_REPLICAS_SETTINGS};
SELECT * FROM v_03733 WHERE a = 0 SETTINGS parallel_replicas_local_plan=0, parallel_replicas_filter_pushdown=0;
" |& grep 'executeQuery' | grep 'HAVING' | wc -l;
