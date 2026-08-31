#!/usr/bin/env bash
# Tags: distributed, no-replicated-database
# A row-hiding `SQL SECURITY NONE` view over `Distributed` must not be rewritten by
# `optimize_trivial_view_pushdown_to_distributed`. That rewrite replaces the view with its inner
# query and reads the `Distributed` table directly, so `StorageView::readImpl` never runs and the
# plan carries no security-barrier step - the invoker's predicate would then be merged with the
# view's own `WHERE` and evaluated on the shards below it. The projection-only twin is the
# positive control: it hides nothing, so it keeps the pushdown.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user05054_${CLICKHOUSE_DATABASE}_$RANDOM"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${db}.t05054_local (id UInt32) ENGINE = MergeTree ORDER BY id;

    CREATE TABLE ${db}.t05054_dist AS ${db}.t05054_local
        ENGINE = Distributed(test_shard_localhost, currentDatabase(), t05054_local);

    INSERT INTO ${db}.t05054_dist VALUES (1), (2), (3);
    SYSTEM FLUSH DISTRIBUTED ${db}.t05054_dist;

    -- The filtering view hides id = 2 from everyone reading through it.
    CREATE VIEW ${db}.v05054_filtering SQL SECURITY NONE
        AS SELECT id FROM ${db}.t05054_dist WHERE id != 2;

    -- The twin hides nothing, so the pushdown stays available for it.
    CREATE VIEW ${db}.v05054_plain SQL SECURITY NONE
        AS SELECT id FROM ${db}.t05054_dist;

    CREATE USER ${user};
    GRANT SELECT ON ${db}.v05054_filtering TO ${user};
    GRANT SELECT ON ${db}.v05054_plain     TO ${user};
"

common_settings="
    SET enable_analyzer = 1;
    SET explain_query_plan_default = 'legacy';
    SET enable_parallel_replicas = 0;
    SET prefer_localhost_replica = 0;
    SET serialize_query_plan = 0;
    SET optimize_trivial_view_pushdown_to_distributed = 1;
"

echo "=== filtering view: pushdown declined, the view stays a subquery ==="
${CLICKHOUSE_CLIENT} --query "
    ${common_settings}
    SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS view_is_a_barrier
    FROM (EXPLAIN SELECT id FROM ${db}.v05054_filtering WHERE id != 3);
"

echo "=== projection-only twin: pushdown still fires ==="
${CLICKHOUSE_CLIENT} --query "
    ${common_settings}
    SELECT countIf(explain LIKE '%VIEW subquery%') = 0 AS pushdown_fires
    FROM (EXPLAIN SELECT id FROM ${db}.v05054_plain WHERE id != 3);
"

echo "=== results are unchanged for the invoker without access to the table ==="
${CLICKHOUSE_CLIENT} --user "${user}" --query "
    ${common_settings}
    SELECT groupArray(id) FROM (SELECT id FROM ${db}.v05054_filtering ORDER BY id);
    SELECT groupArray(id) FROM (SELECT id FROM ${db}.v05054_plain ORDER BY id);
"

${CLICKHOUSE_CLIENT} --query "
    DROP VIEW  ${db}.v05054_filtering;
    DROP VIEW  ${db}.v05054_plain;
    DROP TABLE ${db}.t05054_dist;
    DROP TABLE ${db}.t05054_local;
    DROP USER  ${user};
"
