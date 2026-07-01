#!/usr/bin/env bash

# Regression: the optimized `parallel_distributed_insert_select` paths in `StorageDistributed`
# (`distributedWriteBetweenDistributedTables` for a `Distributed` source, and
# `distributedWriteFromClusterStorage` for an `IStorageCluster` source) forward the per-shard query
# via `RemoteQueryExecutor` using `Context::createCopy(local_context)`. They used to copy that context
# verbatim, so the initiator-only format settings (`input_format` / `output_format` / `default_format`)
# and the HTTP/path-only settings (`http_allow_*` / `implicit_table_at_top_level`) leaked to the
# shards — they are irrelevant to a remote query, and on a rolling upgrade an older shard rejects the
# unknown setting names with `UNKNOWN_SETTING`. Both paths now call
# `ClusterProxy::stripInitiatorOnlySettings`, the same contract as the regular fan-out and
# `DistributedSink` (covered by `04424_distributed_insert_strips_initiator_only_settings`, which
# exercises only the `DistributedSink` path). This test exercises the two `parallel_distributed_insert_select`
# `RemoteQueryExecutor` paths and the `IStorageCluster` SELECT read path (`ReadFromCluster`, which also sends
# the query via `formatWithSecretsOneLine()`), and checks the shard-side queries no longer carry those settings.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS dist_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS dist_dst"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dst (x UInt64) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "INSERT INTO src SELECT number FROM numbers(10)"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dist_src AS src ENGINE = Distributed(test_cluster_two_shards_localhost, ${CLICKHOUSE_DATABASE}, src, x)"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE dist_dst AS dst ENGINE = Distributed(test_cluster_two_shards_localhost, ${CLICKHOUSE_DATABASE}, dst, x)"

# The secondary (shard-side) INSERTs run as the `default` user, so their `current_database` is
# `default`, not the test database — they cannot be filtered by `current_database = currentDatabase()`
# directly. Match them by `initial_query_id`, and apply the (style-check-required)
# `current_database = currentDatabase()` filter to the initial INSERT, which does run in the test db.
check_no_leak() {
    local query_id="$1"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} -q "
        WITH initial AS
        (
            SELECT query_id
            FROM system.query_log
            WHERE current_database = currentDatabase()
              AND query_id = '${query_id}'
              AND is_initial_query = 1
              AND type = 'QueryFinish'
              AND event_date >= today() - 1
        )
        SELECT
            count() >= 1 AS ran_on_shards,
            countIf(Settings['input_format'] != '') AS leaked_input_format,
            countIf(Settings['output_format'] != '') AS leaked_output_format,
            countIf(Settings['default_format'] != '') AS leaked_default_format,
            -- these 3 are forced to '1' by the shard default profile (http_paths.xml); a leak forwards
            -- the initiator explicit '0' and overrides that, so '0' (not mere presence) is the leak signal
            countIf(Settings['http_allow_database_as_path'] = '0') AS leaked_http_allow_database_as_path,
            countIf(Settings['http_allow_table_as_file'] = '0') AS leaked_http_allow_table_as_file,
            countIf(Settings['http_allow_filters_as_path'] = '0') AS leaked_http_allow_filters_as_path,
            countIf(Settings['http_allow_filters_as_unrecognized_url_parameters'] != '') AS leaked_http_filters_as_url_params,
            countIf(Settings['implicit_table_at_top_level'] != '') AS leaked_implicit_table_at_top_level
        FROM system.query_log
        WHERE initial_query_id IN (SELECT query_id FROM initial)
          AND is_initial_query = 0
          AND query_kind = 'Insert'
          AND type = 'QueryFinish'
          AND event_date >= today() - 1"
}

# Initiator-only settings to set on the INSERT. `prefer_localhost_replica = 0` forces every shard
# (both localhost) through the remote-connection path that forwards the settings packet.
# `implicit_table_at_top_level` is set to an existing table (`src`) so it would resolve even if it
# were applied; it is a no-op here because the source SELECT already has a FROM, and we only assert it
# does not reach the shards. (`offset` / `limit` and `compression` are omitted: the former are
# construction settings that materialize into the AST rather than ride along as plain settings, the
# latter is rejected in an in-query `SETTINGS` clause — `04424` covers them for the sink path.)
#
# `http_allow_database_as_path` / `http_allow_table_as_file` / `http_allow_filters_as_path` are
# force-enabled to `1` in the shard's own `<default>` profile (tests/config/users.d/http_paths.xml),
# so the shard-side query carries them via that profile regardless of any leak — "is the setting
# present" cannot distinguish a leak from the profile baseline. We therefore set them to `0` here (a
# real change, since the profile baseline is `1`) and assert above that the shard did NOT receive
# `'0'`: a working strip leaves the shard at its profile value `1`, while a regression that drops the
# strip forwards `0` and overrides the profile. The remaining settings are in no profile, so the
# plain "present at all" check stays valid for them.
LEAK_SETTINGS="parallel_distributed_insert_select = 2, prefer_localhost_replica = 0, log_queries = 1, \
    input_format = 'CSV', output_format = 'CSV', default_format = 'CSV', \
    http_allow_database_as_path = 0, http_allow_table_as_file = 0, http_allow_filters_as_path = 0, \
    http_allow_filters_as_unrecognized_url_parameters = 1, implicit_table_at_top_level = 'src'"

echo "-- distributedWriteBetweenDistributedTables (Distributed source)"
QID_BETWEEN="04492-between-${CLICKHOUSE_DATABASE}-$$"
${CLICKHOUSE_CLIENT} --query_id="${QID_BETWEEN}" -q "
    INSERT INTO dist_dst
    SETTINGS ${LEAK_SETTINGS}
    SELECT * FROM dist_src"
check_no_leak "${QID_BETWEEN}"

echo "-- distributedWriteFromClusterStorage (fileCluster source)"
mkdir -p "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
DATA_FILE="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/data.csv"
${CLICKHOUSE_CLIENT} -q "SELECT number FROM numbers(10) FORMAT CSV" > "${DATA_FILE}"

QID_CLUSTER="04492-cluster-${CLICKHOUSE_DATABASE}-$$"
${CLICKHOUSE_CLIENT} --query_id="${QID_CLUSTER}" -q "
    INSERT INTO dist_dst
    SETTINGS ${LEAK_SETTINGS}
    SELECT * FROM fileCluster('test_cluster_two_shards_localhost', '${CLICKHOUSE_TEST_UNIQUE_NAME}/data.csv', 'CSV', 'x UInt64')"
check_no_leak "${QID_CLUSTER}"

echo "-- initiator-only setting reset to DEFAULT must not ride along in the forwarded query text"
# A `name = DEFAULT` entry lives in `ASTSetQuery::default_settings` (a list separate from `changes`) and is
# serialized by `formatImpl`, so the query-text strip has to clear it too. The reset is applied with
# `changed = false`, so it never travels in the settings packet — it is observable only in the forwarded SQL
# text (`system.query_log.query`), which is precisely what an older shard would reject on parse. Hence this
# case asserts on the query text rather than on the `Settings` map.
QID_DEFAULT="04492-default-${CLICKHOUSE_DATABASE}-$$"
${CLICKHOUSE_CLIENT} --query_id="${QID_DEFAULT}" -q "
    INSERT INTO dist_dst
    SETTINGS parallel_distributed_insert_select = 2, prefer_localhost_replica = 0, log_queries = 1,
        http_allow_table_as_file = DEFAULT, input_format = DEFAULT
    SELECT * FROM dist_src"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} -q "
    WITH initial AS
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND query_id = '${QID_DEFAULT}'
          AND is_initial_query = 1
          AND type = 'QueryFinish'
          AND event_date >= today() - 1
    )
    SELECT
        count() >= 1 AS ran_on_shards,
        countIf(query LIKE '%http_allow_table_as_file%') AS leaked_http_allow_table_as_file_default,
        countIf(query LIKE '%input_format%') AS leaked_input_format_default
    FROM system.query_log
    WHERE initial_query_id IN (SELECT query_id FROM initial)
      AND is_initial_query = 0
      AND query_kind = 'Insert'
      AND type = 'QueryFinish'
      AND event_date >= today() - 1"

echo "-- ReadFromCluster (a SELECT via the fileCluster IStorageCluster) does not forward initiator-only settings in the query text"
# IStorageCluster::read strips the inter-server settings packet (ReadFromCluster::updateSettings) but also
# sends the query via formatWithSecretsOneLine(); an initiator-only setting in the SELECT's own SETTINGS
# clause must not ride along in that forwarded text. Asserted on system.query_log.query, as for the DEFAULT
# case above. (`cluster()` / `clusterAllReplicas()` are StorageDistributed, not IStorageCluster — the
# file/data-lake `*Cluster` functions like `fileCluster` are the ones that go through ReadFromCluster.)
QID_READ="04492-read-${CLICKHOUSE_DATABASE}-$$"
${CLICKHOUSE_CLIENT} --query_id="${QID_READ}" -q "
    SELECT count() FROM fileCluster('test_cluster_two_shards_localhost', '${CLICKHOUSE_TEST_UNIQUE_NAME}/data.csv', 'CSV', 'x UInt64')
    SETTINGS prefer_localhost_replica = 0, log_queries = 1, http_allow_table_as_file = 1, input_format = 'TSV'" > /dev/null
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} -q "
    WITH initial AS
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND query_id = '${QID_READ}'
          AND is_initial_query = 1
          AND type = 'QueryFinish'
          AND event_date >= today() - 1
    )
    SELECT
        count() >= 1 AS ran_on_shards,
        countIf(query LIKE '%http_allow_table_as_file%') AS leaked_http_allow_table_as_file,
        countIf(query LIKE '%input_format%') AS leaked_input_format
    FROM system.query_log
    WHERE initial_query_id IN (SELECT query_id FROM initial)
      AND is_initial_query = 0
      AND query_kind = 'Select'
      AND type = 'QueryFinish'
      AND event_date >= today() - 1"

echo "-- an initiator-only setting on the source SELECT (not the INSERT) is stripped from the forwarded query text"
# `ParserInsertQuery` copies the source SELECT's SETTINGS onto the INSERT (so the optimized path still
# triggers) but leaves the SELECT's own SETTINGS node in place, and `formatImpl` serializes it — so the
# strip has to reach the SELECT too. (Asserted on the forwarded query text.)
QID_SELECT="04492-select-${CLICKHOUSE_DATABASE}-$$"
${CLICKHOUSE_CLIENT} --query_id="${QID_SELECT}" -q "
    INSERT INTO dist_dst
    SELECT * FROM dist_src
    SETTINGS parallel_distributed_insert_select = 2, prefer_localhost_replica = 0, log_queries = 1, input_format = 'CSV', http_allow_table_as_file = 1"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} -q "
    WITH initial AS
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND query_id = '${QID_SELECT}'
          AND is_initial_query = 1
          AND type = 'QueryFinish'
          AND event_date >= today() - 1
    )
    SELECT
        count() >= 1 AS ran_on_shards,
        countIf(query LIKE '%input_format%') AS leaked_input_format,
        countIf(query LIKE '%http_allow_table_as_file%') AS leaked_http_allow_table_as_file
    FROM system.query_log
    WHERE initial_query_id IN (SELECT query_id FROM initial)
      AND is_initial_query = 0
      AND query_kind = 'Insert'
      AND type = 'QueryFinish'
      AND event_date >= today() - 1"

echo "-- an initiator-only setting in a NESTED source subquery is stripped from the forwarded query text"
# The strip must recurse through the rebuilt arm's subtree, not just its top-level SETTINGS, so a nested
# source subquery (WHERE x IN (SELECT ... SETTINGS ...)) does not forward the name either.
QID_NESTED="04492-nested-${CLICKHOUSE_DATABASE}-$$"
${CLICKHOUSE_CLIENT} --query_id="${QID_NESTED}" -q "
    INSERT INTO dist_dst
    SETTINGS parallel_distributed_insert_select = 2, prefer_localhost_replica = 0, log_queries = 1
    SELECT * FROM dist_src WHERE x IN (SELECT x FROM src SETTINGS http_allow_table_as_file = 1)"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} -q "
    WITH initial AS
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND query_id = '${QID_NESTED}'
          AND is_initial_query = 1
          AND type = 'QueryFinish'
          AND event_date >= today() - 1
    )
    SELECT
        count() >= 1 AS ran_on_shards,
        countIf(query LIKE '%http_allow_table_as_file%') AS leaked_http_allow_table_as_file
    FROM system.query_log
    WHERE initial_query_id IN (SELECT query_id FROM initial)
      AND is_initial_query = 0
      AND query_kind = 'Insert'
      AND type = 'QueryFinish'
      AND event_date >= today() - 1"

rm -f "${DATA_FILE}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE dist_dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE dist_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE dst"
${CLICKHOUSE_CLIENT} -q "DROP TABLE src"
