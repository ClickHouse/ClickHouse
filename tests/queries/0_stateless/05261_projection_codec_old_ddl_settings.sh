#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree

# Version 1 distributed DDL omits query settings. Fresh projection codecs that need the
# initiator's settings must be refused before enqueueing; existing metadata can be reused.
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -euo pipefail

create_table="${CLICKHOUSE_DATABASE}.t_projection_codec_old_create"
attach_table="${CLICKHOUSE_DATABASE}.t_projection_codec_old_attach"
attach_uuid=$(${CLICKHOUSE_CLIENT} -q 'SELECT generateUUIDv4()')
source_table="${CLICKHOUSE_DATABASE}.t_projection_codec_old_source"
copy_table="${CLICKHOUSE_DATABASE}.t_projection_codec_old_copy"
normalized_copy_table="${CLICKHOUSE_DATABASE}.t_projection_codec_normalized_copy"
alter_table="${CLICKHOUSE_DATABASE}.t_projection_codec_old_alter"

v1=(--distributed_ddl_entry_format_version=1 --distributed_ddl_task_timeout=0
    --distributed_ddl_output_mode=none --allow_projection_column_list_in_replicated_metadata=1
    --allow_suspicious_codecs=1)
v2=(--distributed_ddl_entry_format_version=2 --distributed_ddl_task_timeout=180
    --distributed_ddl_output_mode=throw --allow_projection_column_list_in_replicated_metadata=1
    --allow_suspicious_codecs=1)
v2_copy=(--distributed_ddl_entry_format_version=2 --distributed_ddl_task_timeout=180
    --distributed_ddl_output_mode=throw --allow_projection_column_list_in_replicated_metadata=1)
v3_copy=(--distributed_ddl_entry_format_version=3 --distributed_ddl_task_timeout=180
    --distributed_ddl_output_mode=throw --allow_projection_column_list_in_replicated_metadata=1)
v1_wait=(--distributed_ddl_entry_format_version=1 --distributed_ddl_task_timeout=180
    --distributed_ddl_output_mode=throw --allow_projection_column_list_in_replicated_metadata=1)
wait_for_worker=(--distributed_ddl_task_timeout=180 --distributed_ddl_output_mode=throw)

expect_disabled_before_enqueue() {
    local label="$1" query="$2" output
    if output=$(${CLICKHOUSE_CLIENT} "${v1[@]}" -q "$query" 2>&1); then
        echo "$label unexpectedly succeeded" >&2
        exit 1
    fi
    if [[ "$output" != *SUPPORT_IS_DISABLED* ]]; then
        echo "$label failed with an unexpected error: $output" >&2
        exit 1
    fi
    echo "$label rejected"
}

expect_disabled_before_enqueue create "
    CREATE TABLE ${create_table} ON CLUSTER test_shard_localhost
        (k UInt64, x UInt64, PROJECTION p (x CODEC(Delta, Delta)) AS (SELECT k, x ORDER BY k))
        ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables
    WHERE database = currentDatabase() AND name = 't_projection_codec_old_create'"

expect_disabled_before_enqueue attach "
    ATTACH TABLE ${attach_table} UUID '${attach_uuid}' ON CLUSTER test_shard_localhost
        (k UInt64, x UInt64, PROJECTION p (x CODEC(Delta, Delta)) AS (SELECT k, x ORDER BY k))
        ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables
    WHERE database = currentDatabase() AND name = 't_projection_codec_old_attach'"

# The AS source definition is copied on the worker without validating its already accepted codec.
${CLICKHOUSE_CLIENT} --allow_suspicious_codecs=1 -q "
    CREATE TABLE ${source_table}
        (k UInt64, x UInt64, PROJECTION p (x CODEC(Delta, Delta)) AS (SELECT k, x ORDER BY k))
        ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} "${v1_wait[@]}" -q "
    CREATE TABLE ${copy_table} ON CLUSTER test_shard_localhost AS ${source_table}
        ENGINE = MergeTree ORDER BY k FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.projections
    WHERE database = currentDatabase() AND table = 't_projection_codec_old_copy'"
${CLICKHOUSE_CLIENT} -q "SELECT countIf(position(replaceAll(create_table_query, '\`', ''),
    'PROJECTION p (x CODEC(Delta, Delta)) AS') > 0) FROM system.tables
    WHERE database = currentDatabase() AND name = 't_projection_codec_old_copy'"

# MODIFY PROJECTION only changes settings; it restates the codec without validating it again.
${CLICKHOUSE_CLIENT} "${v1_wait[@]}" -q "
    ALTER TABLE ${copy_table} ON CLUSTER test_shard_localhost
        MODIFY PROJECTION p (x CODEC(Delta, Delta)) AS (SELECT k, x ORDER BY k)
        WITH SETTINGS (index_granularity = 128) FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT countIf(position(create_table_query,
    'WITH SETTINGS (index_granularity = 128)') > 0) FROM system.tables
    WHERE database = currentDatabase() AND name = 't_projection_codec_old_copy'"
${CLICKHOUSE_CLIENT} "${wait_for_worker[@]}" -q "DROP TABLE ${copy_table} ON CLUSTER test_shard_localhost FORMAT Null"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${alter_table} (k UInt64, x Float64)
    ENGINE = MergeTree ORDER BY k"
expect_disabled_before_enqueue alter_add "
    ALTER TABLE ${alter_table} ON CLUSTER test_shard_localhost
        ADD PROJECTION p (x CODEC(Gorilla)) AS (SELECT k, x ORDER BY k)"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.projections
    WHERE database = currentDatabase() AND table = 't_projection_codec_old_alter'"

# Version 2 carries the initiator's settings to the worker. Explicit declarations still need
# the initiating session's codec override, while copying an existing declaration does not.
if output=$(${CLICKHOUSE_CLIENT} "${v2_copy[@]}" -q "
    CREATE TABLE ${create_table} ON CLUSTER test_shard_localhost
        (k UInt64, x UInt64, PROJECTION p (x CODEC(Delta, Delta)) AS (SELECT k, x ORDER BY k))
        ENGINE = MergeTree ORDER BY k" 2>&1); then
    echo "create_v2_without_codec_setting unexpectedly succeeded" >&2
    exit 1
fi
if [[ "$output" != *BAD_ARGUMENTS* ]]; then
    echo "create_v2_without_codec_setting failed with an unexpected error: $output" >&2
    exit 1
fi
echo "create_v2_without_codec_setting rejected"

${CLICKHOUSE_CLIENT} "${v2[@]}" -q "
    CREATE TABLE ${create_table} ON CLUSTER test_shard_localhost
        (k UInt64, x UInt64, PROJECTION p (x CODEC(Delta, Delta)) AS (SELECT k, x ORDER BY k))
        ENGINE = MergeTree ORDER BY k FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.projections
    WHERE database = currentDatabase() AND table = 't_projection_codec_old_create'"
${CLICKHOUSE_CLIENT} -q "SELECT countIf(position(replaceAll(create_table_query, '\`', ''),
    'PROJECTION p (x CODEC(Delta, Delta)) AS') > 0) FROM system.tables
    WHERE database = currentDatabase() AND name = 't_projection_codec_old_create'"

${CLICKHOUSE_CLIENT} "${v2[@]}" -q "
    ATTACH TABLE ${attach_table} UUID '${attach_uuid}' ON CLUSTER test_shard_localhost
        (k UInt64, x UInt64, PROJECTION p (x CODEC(Delta, Delta)) AS (SELECT k, x ORDER BY k))
        ENGINE = MergeTree ORDER BY k FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.projections
    WHERE database = currentDatabase() AND table = 't_projection_codec_old_attach'"
${CLICKHOUSE_CLIENT} -q "SELECT countIf(position(replaceAll(create_table_query, '\`', ''),
    'PROJECTION p (x CODEC(Delta, Delta)) AS') > 0) FROM system.tables
    WHERE database = currentDatabase() AND name = 't_projection_codec_old_attach'"

${CLICKHOUSE_CLIENT} "${v2_copy[@]}" -q "
    CREATE TABLE ${copy_table} ON CLUSTER test_shard_localhost AS ${source_table}
        ENGINE = MergeTree ORDER BY k FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.projections
    WHERE database = currentDatabase() AND table = 't_projection_codec_old_copy'"
${CLICKHOUSE_CLIENT} -q "SELECT countIf(position(replaceAll(create_table_query, '\`', ''),
    'PROJECTION p (x CODEC(Delta, Delta)) AS') > 0) FROM system.tables
    WHERE database = currentDatabase() AND name = 't_projection_codec_old_copy'"

# Format 3 normalizes AS source into an explicit projection list before enqueueing it. The worker
# must reuse the accepted codec even though the initiating session no longer allows that spelling.
if output=$(${CLICKHOUSE_CLIENT} "${v3_copy[@]}" -q "
    CREATE TABLE ${normalized_copy_table} ON CLUSTER test_shard_localhost
        (k UInt64, x UInt64, PROJECTION p (x CODEC(Delta, Delta)) AS (SELECT k, x ORDER BY k))
        ENGINE = MergeTree ORDER BY k" 2>&1); then
    echo "create_v3_without_codec_setting unexpectedly succeeded" >&2
    exit 1
fi
if [[ "$output" != *BAD_ARGUMENTS* ]]; then
    echo "create_v3_without_codec_setting failed with an unexpected error: $output" >&2
    exit 1
fi
echo "create_v3_without_codec_setting rejected"

${CLICKHOUSE_CLIENT} "${v3_copy[@]}" -q "
    CREATE TABLE ${normalized_copy_table} ON CLUSTER test_shard_localhost AS ${source_table}
        ENGINE = MergeTree ORDER BY k FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.projections
    WHERE database = currentDatabase() AND table = 't_projection_codec_normalized_copy'"
${CLICKHOUSE_CLIENT} -q "SELECT countIf(position(replaceAll(create_table_query, '\`', ''),
    'PROJECTION p (x CODEC(Delta, Delta)) AS') > 0) FROM system.tables
    WHERE database = currentDatabase() AND name = 't_projection_codec_normalized_copy'"

${CLICKHOUSE_CLIENT} "${v2[@]}" -q "
    ALTER TABLE ${alter_table} ON CLUSTER test_shard_localhost
        ADD PROJECTION p (x CODEC(Gorilla)) AS (SELECT k, x ORDER BY k) FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.projections
    WHERE database = currentDatabase() AND table = 't_projection_codec_old_alter'"

${CLICKHOUSE_CLIENT} "${wait_for_worker[@]}" --distributed_ddl_entry_format_version=1 -q "
    ALTER TABLE ${alter_table} ON CLUSTER test_shard_localhost MODIFY COLUMN x UInt64 FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT type FROM system.columns
    WHERE database = currentDatabase() AND table = 't_projection_codec_old_alter' AND name = 'x'"

${CLICKHOUSE_CLIENT} "${v2[@]}" -q "
    ALTER TABLE ${alter_table} ON CLUSTER test_shard_localhost MODIFY COLUMN x Float64 FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT type FROM system.columns
    WHERE database = currentDatabase() AND table = 't_projection_codec_old_alter' AND name = 'x'"

${CLICKHOUSE_CLIENT} "${wait_for_worker[@]}" -q "DROP TABLE ${create_table} ON CLUSTER test_shard_localhost FORMAT Null"
${CLICKHOUSE_CLIENT} "${wait_for_worker[@]}" -q "DROP TABLE ${attach_table} ON CLUSTER test_shard_localhost FORMAT Null"
${CLICKHOUSE_CLIENT} "${wait_for_worker[@]}" -q "DROP TABLE ${copy_table} ON CLUSTER test_shard_localhost FORMAT Null"
${CLICKHOUSE_CLIENT} "${wait_for_worker[@]}" -q "DROP TABLE ${normalized_copy_table} ON CLUSTER test_shard_localhost FORMAT Null"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${source_table}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${alter_table}"
