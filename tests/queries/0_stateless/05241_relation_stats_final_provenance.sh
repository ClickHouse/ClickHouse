#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery --query "
    DROP TABLE IF EXISTS t_relation_stats_final_l;
    DROP TABLE IF EXISTS t_relation_stats_final_r;

    CREATE TABLE t_relation_stats_final_l (a1 UInt32, a2 UInt32, a3 UInt32)
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS auto_statistics_types = 'basic, uniq_v2';

    CREATE TABLE t_relation_stats_final_r (k UInt32, b1 UInt32, b2 UInt32, b3 UInt32)
    ENGINE = SummingMergeTree ORDER BY k
    SETTINGS auto_statistics_types = 'basic, uniq_v2';

    SYSTEM STOP MERGES t_relation_stats_final_r;
    SET materialize_statistics_on_insert = 1;
    INSERT INTO t_relation_stats_final_l SELECT number, number, number FROM numbers(10);
    INSERT INTO t_relation_stats_final_r VALUES (1, 60, 60, 60);
    INSERT INTO t_relation_stats_final_r VALUES (1, 60, 60, 60);
"

$CLICKHOUSE_CLIENT --query "
    EXPLAIN actions = 1
    SELECT count()
    FROM t_relation_stats_final_l AS l
    JOIN (SELECT b1, b2, b3 FROM t_relation_stats_final_r FINAL) AS r
        ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
    SETTINGS
        join_algorithm = 'ie_join',
        join_use_nulls = 0,
        use_statistics = 1,
        query_plan_optimize_join_order_limit = 0,
        enable_parallel_replicas = 0
" --send_logs_level='trace' --send_logs_source_regexp='optimizeJoin' 2>&1 \
    | grep 'estimate statistics t_relation_stats_final_r:' \
    | grep -oF 'b3: 1 (ndv: part-statistics[row-subset,unsupported], range: part-statistics[row-subset,unsupported])'

$CLICKHOUSE_CLIENT --multiquery --query "
    DROP TABLE t_relation_stats_final_l;
    DROP TABLE t_relation_stats_final_r;
"
