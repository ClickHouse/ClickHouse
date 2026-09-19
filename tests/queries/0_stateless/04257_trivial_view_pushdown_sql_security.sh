#!/usr/bin/env bash
# Tags: distributed, no-replicated-database
# Tests that optimize_trivial_view_pushdown_to_distributed respects sql_security_type:
#   1. SQL SECURITY INVOKER: user with SELECT on the view but not on the underlying
#      distributed table must get "Not enough privileges" (privilege check fix).
#   2. SQL SECURITY INVOKER: the access check must be pruned to the columns the outer
#      query actually reads from the view, not every column the view body mentions;
#      matches readImpl, which forwards the outer column list into its inner
#      interpreter and lets the analyzer drop unreferenced columns.
#   3. SQL SECURITY NONE: the view is an optimization barrier (see
#      `sql_security_views_are_optimization_barriers`), so the pushdown is declined and the view is
#      read through `StorageView::readImpl` under the no-user context; a user with SELECT on the view
#      but not on the underlying Distributed table still succeeds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user04257_${CLICKHOUSE_DATABASE}_$RANDOM"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS ${db}.t04257_local;
    DROP TABLE IF EXISTS ${db}.t04257_dist;
    DROP VIEW  IF EXISTS ${db}.v04257_invoker;
    DROP VIEW  IF EXISTS ${db}.v04257_invoker_subset;
    DROP USER  IF EXISTS ${user};

    CREATE TABLE ${db}.t04257_local (id UInt32, secret UInt8 DEFAULT 0)
        ENGINE = MergeTree ORDER BY id;

    CREATE TABLE ${db}.t04257_dist AS ${db}.t04257_local
        ENGINE = Distributed(test_shard_localhost, currentDatabase(), t04257_local);

    INSERT INTO ${db}.t04257_dist (id) VALUES (1), (2), (3);
    SYSTEM FLUSH DISTRIBUTED ${db}.t04257_dist;

    CREATE VIEW ${db}.v04257_invoker SQL SECURITY INVOKER
        AS SELECT id FROM ${db}.t04257_dist;

    CREATE VIEW ${db}.v04257_invoker_subset SQL SECURITY INVOKER
        AS SELECT id, secret FROM ${db}.t04257_dist;

    CREATE USER ${user};
    GRANT SELECT ON ${db}.v04257_invoker        TO ${user};
    GRANT SELECT ON ${db}.v04257_invoker_subset TO ${user};
"

# -------------------------------------------------------------------
# Scenario 1: SQL SECURITY INVOKER — privilege check must fire.
# The user has SELECT on the view but not on t04257_dist, so the
# pushdown path must raise an access-denied exception.
# -------------------------------------------------------------------
echo "=== INVOKER: privilege check ==="
# serialize_query_plan = 0 is set explicitly because the "distributed plan"
# CI shard enables it in the default profile, which triggers a pre-existing
# coordinator-side access check on the substituted shard-local table that is
# unrelated to this optimization.
${CLICKHOUSE_CLIENT} \
    --user "${user}" \
    --query "
        SET enable_analyzer = 1;
        SET enable_parallel_replicas = 0;
        SET prefer_localhost_replica = 0;
        SET serialize_query_plan = 0;
        SET optimize_trivial_view_pushdown_to_distributed = 1;
        SELECT id FROM ${db}.v04257_invoker ORDER BY id;
    " 2>&1 | grep -o "Not enough privileges" | head -1

# -------------------------------------------------------------------
# Scenario 2: SQL SECURITY INVOKER — the access check must be bounded
# to columns the outer query actually reads. View body projects
# `id, secret`; outer reads only `id`; the user holds SELECT(id) but
# not SELECT(secret). Without column pruning, the access check would
# over-require SELECT(secret) and reject the user.
# -------------------------------------------------------------------
${CLICKHOUSE_CLIENT} --query "
    GRANT SELECT(id) ON ${db}.t04257_dist TO ${user};
"

echo "=== INVOKER: access check pruned to outer-referenced columns ==="
${CLICKHOUSE_CLIENT} \
    --user "${user}" \
    --query "
        SET enable_analyzer = 1;
        SET enable_parallel_replicas = 0;
        SET prefer_localhost_replica = 0;
        SET serialize_query_plan = 0;
        SET optimize_trivial_view_pushdown_to_distributed = 1;
        SELECT id FROM ${db}.v04257_invoker_subset ORDER BY id;
    "

# -------------------------------------------------------------------
# Scenario 3: SQL SECURITY NONE — the view is an optimization barrier,
# so the pushdown is declined: a shard runs the shipped query as the
# cluster's user, and a row policy on the shard-local table could hide
# rows that the invoker's pushed-down predicate would then probe. The
# view is read through StorageView::readImpl under the no-user/global
# context instead, so a user with SELECT on the view but not on the
# underlying Distributed table must still succeed.
# -------------------------------------------------------------------

# Revoke the per-column grant added in Scenario 2; the user must have
# no direct access to t04257_dist for this scenario to be meaningful.
${CLICKHOUSE_CLIENT} --query "
    REVOKE SELECT ON ${db}.t04257_dist FROM ${user};

    DROP VIEW  IF EXISTS ${db}.v04257_none;
    CREATE VIEW ${db}.v04257_none SQL SECURITY NONE
        AS SELECT id FROM ${db}.t04257_dist;
    GRANT SELECT ON ${db}.v04257_none TO ${user};
"

echo "=== NONE: direct table access denied ==="
${CLICKHOUSE_CLIENT} \
    --user "${user}" \
    --query "
        SET enable_analyzer = 1;
        SET enable_parallel_replicas = 0;
        SET prefer_localhost_replica = 0;
        SELECT id FROM ${db}.t04257_dist ORDER BY id;
    " 2>&1 | grep -o "Not enough privileges" | head -1

echo "=== NONE: view access succeeds ==="
${CLICKHOUSE_CLIENT} \
    --user "${user}" \
    --query "
        SET enable_analyzer = 1;
        SET enable_parallel_replicas = 0;
        SET prefer_localhost_replica = 0;
        SET serialize_query_plan = 0;
        SET optimize_trivial_view_pushdown_to_distributed = 1;
        SELECT id FROM ${db}.v04257_none ORDER BY id;
    "

echo "=== NONE: pushdown declined (the VIEW subquery step stays) ==="
${CLICKHOUSE_CLIENT} \
    --query "
        SET enable_analyzer = 1;
        SET explain_query_plan_default = 'legacy';
        SET enable_parallel_replicas = 0;
        SET prefer_localhost_replica = 0;
        SET optimize_trivial_view_pushdown_to_distributed = 1;
        SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS view_is_a_barrier
        FROM (EXPLAIN SELECT id FROM ${db}.v04257_none);
    "

${CLICKHOUSE_CLIENT} --query "
    DROP VIEW  IF EXISTS ${db}.v04257_invoker;
    DROP VIEW  IF EXISTS ${db}.v04257_invoker_subset;
    DROP VIEW  IF EXISTS ${db}.v04257_none;
    DROP TABLE IF EXISTS ${db}.t04257_dist;
    DROP TABLE IF EXISTS ${db}.t04257_local;
    DROP USER  IF EXISTS ${user};
"
