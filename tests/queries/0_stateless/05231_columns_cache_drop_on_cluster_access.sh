#!/usr/bin/env bash
# `SYSTEM DROP COLUMNS CACHE` is checked in two places: the local execution path, and the
# initiator-side mapping of `ON CLUSTER` queries in
# `InterpreterSystemQuery::getRequiredAccessForDDLOnCluster`. The local spelling is covered by
# 05023; here the `ON CLUSTER` spelling must require exactly the same granular privilege - not
# the whole `DROP CACHE` group, and not nothing at all.
# Tags: no-parallel
# The test drops the process-wide columns cache.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

granular_user="granular_user_05231_${CLICKHOUSE_DATABASE}"
cluster_only_user="cluster_only_user_05231_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${granular_user}, ${cluster_only_user}"

$CLICKHOUSE_CLIENT -q "CREATE USER ${granular_user}"
$CLICKHOUSE_CLIENT -q "GRANT CLUSTER, SYSTEM DROP COLUMNS CACHE ON *.* TO ${granular_user}"

# Negative control: `CLUSTER` alone.
$CLICKHOUSE_CLIENT -q "CREATE USER ${cluster_only_user}"
$CLICKHOUSE_CLIENT -q "GRANT CLUSTER ON *.* TO ${cluster_only_user}"

cluster="test_shard_localhost"
run() { $CLICKHOUSE_CLIENT --distributed_ddl_output_mode none "$@"; }

# The granular privilege is enough: the access check runs on the initiator before the task is
# enqueued, so a denial would never reach a host.
run --user "${granular_user}" -q "SYSTEM DROP COLUMNS CACHE ON CLUSTER ${cluster}" || exit 1
echo "granular privilege allowed"

# Without it, the initiator denies the query and names the privilege it required, so the mapping
# itself is asserted rather than only its outcome.
out=$(run --user "${cluster_only_user}" -q "SYSTEM DROP COLUMNS CACHE ON CLUSTER ${cluster}" 2>&1)
echo "cluster only -> $(sed -n "/necessary to have the grant/{s/.*grant \(.*\) ON \*\.\*.*/\1/p;q;}" <<< "$out")"

$CLICKHOUSE_CLIENT -q "DROP USER ${granular_user}, ${cluster_only_user}"
