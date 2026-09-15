#!/usr/bin/env bash
# Tags: no-parallel
# Reason: creates users and uses the server-wide part aggregation cache

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Two independent access surfaces of the part aggregation cache, neither of which is asserted by
# `01271_show_privileges` alone:
#  * `SYSTEM DROP PART AGGREGATION CACHE` must require `SYSTEM DROP PART AGGREGATION CACHE`, both in
#    the local entrypoint checked here and in `getRequiredAccessForDDLOnCluster`, which
#    `04836_system_cache_on_cluster_access_types` asserts for the `ON CLUSTER` spelling.
#  * `system.part_aggregation_cache` exposes the `UUID` and the part names of the tables whose
#    aggregation states are cached, so its rows must be filtered by the querying user's access to
#    the source table.

# The functional-test config (`tests/config/users.d/limits.yaml`) sets `max_rows_to_group_by` and
# read limits, on which the optimization fails closed; pin them to 0 so the cache is exercised
# (as in `04033_part_aggregation_cache`).
CACHE_SETTINGS="allow_experimental_analyzer = 0, allow_experimental_part_aggregation_cache = 1, optimize_aggregation_in_order = 0, enable_memory_bound_merging_of_aggregation_results = 0, max_rows_to_group_by = 0, max_rows_to_read = 0, max_bytes_to_read = 0, max_rows_to_read_leaf = 0, max_bytes_to_read_leaf = 0"

USER_NO_GRANT="u05139_no_grant_${CLICKHOUSE_DATABASE}"
USER_DROP="u05139_drop_${CLICKHOUSE_DATABASE}"
USER_READER="u05139_reader_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER_NO_GRANT}, ${USER_DROP}, ${USER_READER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER_NO_GRANT}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER_DROP}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER_READER}"

echo "-- local drop access"
# Holds nothing: denied.
${CLICKHOUSE_CLIENT} --user "${USER_NO_GRANT}" -q "SYSTEM DROP PART AGGREGATION CACHE" 2>&1 | grep -c -F "ACCESS_DENIED"
# Holds exactly the granular privilege of this command, and nothing else: allowed.
${CLICKHOUSE_CLIENT} -q "GRANT SYSTEM DROP PART AGGREGATION CACHE ON *.* TO ${USER_DROP}"
${CLICKHOUSE_CLIENT} --user "${USER_DROP}" -q "SYSTEM DROP PART AGGREGATION CACHE" && echo "ok"
# The privilege group the command is parented to also covers it.
${CLICKHOUSE_CLIENT} -q "GRANT DROP CACHE ON *.* TO ${USER_NO_GRANT}"
${CLICKHOUSE_CLIENT} --user "${USER_NO_GRANT}" -q "SYSTEM DROP PART AGGREGATION CACHE" && echo "ok"

echo "-- system.part_aggregation_cache access"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_part_agg_cache_access"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_part_agg_cache_access (k UInt32, v UInt64) ENGINE = MergeTree ORDER BY k"
# Keep the part layout stable so the warmed entry stays addressable by part name.
${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES t_part_agg_cache_access"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_part_agg_cache_access SELECT number, number FROM numbers(8)"

${CLICKHOUSE_CLIENT} -q "SYSTEM DROP PART AGGREGATION CACHE"
${CLICKHOUSE_CLIENT} -q "SELECT k, sum(v) FROM t_part_agg_cache_access GROUP BY k ORDER BY k SETTINGS ${CACHE_SETTINGS}" > /dev/null

uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't_part_agg_cache_access'")

# The warmup cached the part, so the owner of the table sees exactly one entry for it.
echo -n "owner sees own entries: "
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.part_aggregation_cache WHERE table_id = '${uuid}'"

# A user who may read the system table but cannot see the source table must not learn its `UUID`
# or part names from it. Before the access filter this printed 1.
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON system.part_aggregation_cache TO ${USER_READER}"
echo -n "reader without SHOW TABLES sees: "
${CLICKHOUSE_CLIENT} --user "${USER_READER}" -q "SELECT count() FROM system.part_aggregation_cache WHERE table_id = '${uuid}'"

# The filter is not vacuous: once the table is visible to the user, its entries are too.
${CLICKHOUSE_CLIENT} -q "GRANT SHOW TABLES ON ${CLICKHOUSE_DATABASE}.t_part_agg_cache_access TO ${USER_READER}"
echo -n "reader with SHOW TABLES sees: "
${CLICKHOUSE_CLIENT} --user "${USER_READER}" -q "SELECT count() FROM system.part_aggregation_cache WHERE table_id = '${uuid}'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_part_agg_cache_access"
${CLICKHOUSE_CLIENT} -q "DROP USER ${USER_NO_GRANT}, ${USER_DROP}, ${USER_READER}"
