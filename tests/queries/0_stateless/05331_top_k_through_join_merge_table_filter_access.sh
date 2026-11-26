#!/usr/bin/env bash

# A user without `SELECT` on a table that the `_table` condition excludes can run the query, the `topKThroughJoin`
# probe does not check access to the excluded table. Reading a table without `SELECT` on it is still denied.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}_05331"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user;
CREATE TABLE t_access_a (k Int64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_access_b (k Int64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_access_right (k Int64, v Int64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_access_merge (k Int64) ENGINE = Merge(currentDatabase(), '^t_access_[ab]\$');
INSERT INTO t_access_a SELECT number FROM numbers(100);
INSERT INTO t_access_b SELECT number + 1000 FROM numbers(100);
INSERT INTO t_access_right SELECT number, number * 10 FROM numbers(2000);
CREATE USER $user IDENTIFIED WITH plaintext_password BY 'password';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_access_merge TO $user;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_access_a TO $user;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_access_right TO $user;
GRANT SHOW TABLES ON ${CLICKHOUSE_DATABASE}.t_access_b TO $user;
"
trap '$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $user"' EXIT

# The settings are pinned for the same reason as in 05331_top_k_through_join_merge_table_filter.
settings="SETTINGS query_plan_top_k_through_join = 1, optimize_read_in_order = 1, query_plan_read_in_order_through_join = 1,
    query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0, enable_join_runtime_filters = 0,
    enable_lazy_columns_replication = 0, query_plan_optimize_lazy_materialization = 0, enable_parallel_replicas = 0,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0"
query="SELECT l.k, r.v FROM t_access_merge AS l LEFT JOIN t_access_right AS r ON r.k = l.k"

$CLICKHOUSE_CLIENT --user "$user" --password password -q "$query WHERE l._table = 't_access_a' ORDER BY l.k DESC LIMIT 3 $settings"
$CLICKHOUSE_CLIENT --user "$user" --password password -q "$query WHERE l._table = 't_access_b' ORDER BY l.k DESC LIMIT 3 $settings" 2>&1 | grep -o -m1 'ACCESS_DENIED'
