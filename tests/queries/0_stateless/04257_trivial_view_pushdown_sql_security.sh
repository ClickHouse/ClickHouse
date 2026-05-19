#!/usr/bin/env bash
# Tags: distributed, no-replicated-database
# Tests that optimize_trivial_view_pushdown_to_distributed respects sql_security_type:
#   1. SQL SECURITY INVOKER: user with SELECT on the view but not on the underlying
#      distributed table must get "Not enough privileges" (privilege check fix).
#   2. SQL SECURITY NONE: a row policy defined for the calling user on the underlying
#      distributed table must NOT be applied (NONE runs with no-user context; row-policy
#      context fix).
#   3. SQL SECURITY INVOKER: a row policy that references a column the user is NOT
#      granted on must not be added to the access-check column set (the check must run
#      on the pre-policy analyzed tree, matching readImpl).
#   4. SQL SECURITY INVOKER: the pre-policy access check must be pruned to the columns
#      the outer query actually reads from the view, not every column the view body
#      mentions; matches readImpl, which forwards the outer column list into its inner
#      interpreter and lets the analyzer drop unreferenced columns.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user04257_${CLICKHOUSE_DATABASE}_$RANDOM"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS ${db}.t04257_local;
    DROP TABLE IF EXISTS ${db}.t04257_dist;
    DROP VIEW  IF EXISTS ${db}.v04257_invoker;
    DROP VIEW  IF EXISTS ${db}.v04257_none;
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

    CREATE VIEW ${db}.v04257_none SQL SECURITY NONE
        AS SELECT id FROM ${db}.t04257_dist;

    CREATE VIEW ${db}.v04257_invoker_subset SQL SECURITY INVOKER
        AS SELECT id, secret FROM ${db}.t04257_dist;

    CREATE USER ${user};
    GRANT SELECT ON ${db}.v04257_invoker        TO ${user};
    GRANT SELECT ON ${db}.v04257_none           TO ${user};
    GRANT SELECT ON ${db}.v04257_invoker_subset TO ${user};
    GRANT ALLOW SQL SECURITY NONE ON *.* TO ${user};
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
# Scenario 2: SQL SECURITY NONE — the calling user's row policy on
# the distributed table must NOT filter results.
# A "TO <user>" policy targets only authenticated users; the NONE
# path executes with a no-user (global) context, so the policy must
# be invisible there.  All 3 rows must be returned.
# -------------------------------------------------------------------
${CLICKHOUSE_CLIENT} --query "
    CREATE ROW POLICY pol04257 ON ${db}.t04257_dist
        USING id != 2 TO ${user};
"

echo "=== NONE: row policy must not apply ==="
${CLICKHOUSE_CLIENT} \
    --user "${user}" \
    --query "
        SET enable_analyzer = 1;
        SET enable_parallel_replicas = 0;
        SET prefer_localhost_replica = 0;
        SET serialize_query_plan = 0;
        SET optimize_trivial_view_pushdown_to_distributed = 1;
        SELECT count() FROM ${db}.v04257_none;
    "

# -------------------------------------------------------------------
# Scenario 3: SQL SECURITY INVOKER — a row policy that references a
# column the user is NOT granted on must not block the user from
# selecting columns they ARE granted on. This matches StorageView::
# readImpl semantics: its access check fires before the planner
# injects row policies, so policy-only columns never enter the grant
# requirement. View body reads `id`; user has GRANT SELECT(id); the
# row policy on t04257_dist references `secret`.
# -------------------------------------------------------------------
${CLICKHOUSE_CLIENT} --query "
    DROP ROW POLICY IF EXISTS pol04257 ON ${db}.t04257_dist;
    INSERT INTO ${db}.t04257_dist (id, secret) VALUES (4, 1);
    SYSTEM FLUSH DISTRIBUTED ${db}.t04257_dist;
    CREATE ROW POLICY pol04257_secret ON ${db}.t04257_dist
        USING secret = 0 TO ${user};
    GRANT SELECT(id) ON ${db}.t04257_dist TO ${user};
"

echo "=== INVOKER: policy-only column does not block column-grant user ==="
${CLICKHOUSE_CLIENT} \
    --user "${user}" \
    --query "
        SET enable_analyzer = 1;
        SET enable_parallel_replicas = 0;
        SET prefer_localhost_replica = 0;
        SET serialize_query_plan = 0;
        SET optimize_trivial_view_pushdown_to_distributed = 1;
        SELECT id FROM ${db}.v04257_invoker ORDER BY id;
    "

# -------------------------------------------------------------------
# Scenario 4: SQL SECURITY INVOKER — the access check must be bounded
# to columns the outer query actually reads. View body projects
# `id, secret`; outer reads only `id`; the user holds SELECT(id) but
# not SELECT(secret). Without column pruning, the pre-policy access
# check would over-require SELECT(secret) and reject the user.
# Drop the leftover row policy from scenario 3 so this case isolates
# column pruning, not policy injection.
# -------------------------------------------------------------------
${CLICKHOUSE_CLIENT} --query "
    DROP ROW POLICY IF EXISTS pol04257_secret ON ${db}.t04257_dist;
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

${CLICKHOUSE_CLIENT} --query "
    DROP ROW POLICY IF EXISTS pol04257_secret ON ${db}.t04257_dist;
    DROP ROW POLICY IF EXISTS pol04257 ON ${db}.t04257_dist;
    DROP VIEW  IF EXISTS ${db}.v04257_invoker;
    DROP VIEW  IF EXISTS ${db}.v04257_none;
    DROP VIEW  IF EXISTS ${db}.v04257_invoker_subset;
    DROP TABLE IF EXISTS ${db}.t04257_dist;
    DROP TABLE IF EXISTS ${db}.t04257_local;
    DROP USER  IF EXISTS ${user};
"
