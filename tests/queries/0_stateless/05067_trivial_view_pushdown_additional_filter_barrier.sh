#!/usr/bin/env bash
# Tags: distributed, no-replicated-database
# `optimize_trivial_view_pushdown_to_distributed` folds an `additional_table_filters` entry keyed by
# the view into the shipped query's `WHERE`, where the invoker's own predicate is free to merge with
# it on the shard. A `SQL SECURITY NONE` / `DEFINER` view must therefore decline the rewrite when
# such an entry applies to it, exactly as it does for a row policy or a filtering view body. Without
# the entry the projection-only view keeps the pushdown - that is the positive control.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user05067_${CLICKHOUSE_DATABASE}_$RANDOM"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${db}.t05067_local (id UInt32) ENGINE = MergeTree ORDER BY id;

    CREATE TABLE ${db}.t05067_dist AS ${db}.t05067_local
        ENGINE = Distributed(test_shard_localhost, currentDatabase(), t05067_local);

    INSERT INTO ${db}.t05067_dist VALUES (1), (2), (3);
    SYSTEM FLUSH DISTRIBUTED ${db}.t05067_dist;

    -- The view body hides nothing: all the row hiding comes from \`additional_table_filters\`.
    CREATE VIEW ${db}.v05067 SQL SECURITY NONE AS SELECT id FROM ${db}.t05067_dist;

    CREATE USER ${user};
    GRANT SELECT ON ${db}.v05067 TO ${user};
"

common_settings="
    SET enable_analyzer = 1;
    SET explain_query_plan_default = 'legacy';
    SET enable_parallel_replicas = 0;
    SET prefer_localhost_replica = 0;
    SET serialize_query_plan = 0;
    SET optimize_trivial_view_pushdown_to_distributed = 1;
"

echo "=== view-keyed additional filter: pushdown declined, the view stays a subquery ==="
${CLICKHOUSE_CLIENT} --query "
    ${common_settings}
    SET additional_table_filters = {'v05067': 'id != 2'};
    SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS view_is_a_barrier
    FROM (EXPLAIN SELECT id FROM ${db}.v05067 WHERE id != 3);
"

echo "=== no additional filter: pushdown still fires ==="
${CLICKHOUSE_CLIENT} --query "
    ${common_settings}
    SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS pushdown_fires
    FROM (EXPLAIN SELECT id FROM ${db}.v05067 WHERE id != 3);
"

echo "=== the filter still applies to the result ==="
${CLICKHOUSE_CLIENT} --user "${user}" --query "
    ${common_settings}
    SET additional_table_filters = {'v05067': 'id != 2'};
    SELECT groupArray(id) FROM (SELECT id FROM ${db}.v05067 ORDER BY id);
"

${CLICKHOUSE_CLIENT} --query "
    DROP VIEW  ${db}.v05067;
    DROP TABLE ${db}.t05067_dist;
    DROP TABLE ${db}.t05067_local;
    DROP USER  ${user};
"
