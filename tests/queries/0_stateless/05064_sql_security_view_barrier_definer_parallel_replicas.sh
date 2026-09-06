#!/usr/bin/env bash
# A `SQL SECURITY DEFINER` view that can hide rows reads its inner query without parallel replicas
# (see `StorageView::readImpl`). The definer's profile is what decides whether parallel replicas
# were on for the inner query, so the switch is applied to the view context - which, for a
# `DEFINER`/`NONE` view, is its own query context. Turning it off on a *copy* of that context used
# to destroy the original, and the inner query then failed to resolve a table function with
# `THERE_IS_NO_QUERY`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

db=${CLICKHOUSE_DATABASE}
definer="definer05064_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${definer}"
$CLICKHOUSE_CLIENT --query "
    CREATE USER ${definer} IDENTIFIED WITH no_password
        SETTINGS enable_parallel_replicas = 1,
                 cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
                 parallel_replicas_for_non_replicated_merge_tree = 1"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON ${db}.* TO ${definer}"
$CLICKHOUSE_CLIENT --query "GRANT CREATE TEMPORARY TABLE ON *.* TO ${definer}"

# A table function is a source `canHideRows` cannot prove row-preserving, so the view is a barrier
# that hides rows and the parallel-replicas fence applies to its inner query.
$CLICKHOUSE_CLIENT --query "
    CREATE VIEW ${db}.v05064 DEFINER = ${definer} SQL SECURITY DEFINER
    AS SELECT number AS x FROM numbers(10)"

# Control: the invoker's own setting change is applied on top of the definer's profile, so the view
# context has parallel replicas off already and the fence has nothing to switch.
echo "--- invoker turns parallel replicas off ---"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM ${db}.v05064 SETTINGS enable_parallel_replicas = 0"

# The fence has to switch them off for the inner query. This is where the destroyed query context
# used to surface.
echo "--- invoker turns parallel replicas on ---"
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM ${db}.v05064
    SETTINGS enable_parallel_replicas = 1,
             cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
             parallel_replicas_for_non_replicated_merge_tree = 1"

$CLICKHOUSE_CLIENT --query "DROP VIEW ${db}.v05064"
$CLICKHOUSE_CLIENT --query "DROP USER ${definer}"
