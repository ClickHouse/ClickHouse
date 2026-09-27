#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree

# `ADD ENUM VALUES` builds a new column type without a type node in the `ALTER` AST. An
# existing projection codec is structural metadata, so this must also work with format-1 DDL.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -euo pipefail

table="${CLICKHOUSE_DATABASE}.t_projection_codec_old_enum"
plain_table="${CLICKHOUSE_DATABASE}.t_projection_codec_old_enum_plain"

${CLICKHOUSE_CLIENT} --allow_suspicious_codecs=1 -q "
    CREATE TABLE ${table}
        (k UInt64, x Enum8('a' = 1), PROJECTION p (x CODEC(Delta, Delta)) AS (SELECT k, x ORDER BY k))
        ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${table} VALUES (1, 'a')"

v1_safe=(--distributed_ddl_entry_format_version=1 --distributed_ddl_task_timeout=180
    --distributed_ddl_output_mode=throw)
v2=(--distributed_ddl_entry_format_version=2 --distributed_ddl_task_timeout=180
    --distributed_ddl_output_mode=throw --allow_projection_column_list_in_replicated_metadata=1
    --allow_suspicious_codecs=1)

# A metadata-only `MODIFY COLUMN` remains available in format 1.
${CLICKHOUSE_CLIENT} "${v1_safe[@]}" -q "
    ALTER TABLE ${table} ON CLUSTER test_shard_localhost
        MODIFY COLUMN x COMMENT 'unchanged type' FORMAT Null"

${CLICKHOUSE_CLIENT} "${v1_safe[@]}" -q "
    ALTER TABLE ${table} ON CLUSTER test_shard_localhost
        MODIFY COLUMN x ADD ENUM VALUES ('b' = 2) FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT type LIKE '%b%' FROM system.columns
    WHERE database = currentDatabase() AND table = 't_projection_codec_old_enum' AND name = 'x'"

${CLICKHOUSE_CLIENT} "${v2[@]}" -q "
    ALTER TABLE ${table} ON CLUSTER test_shard_localhost
        MODIFY COLUMN x ADD ENUM VALUES ('c' = 3) FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT type LIKE '%c%' FROM system.columns
    WHERE database = currentDatabase() AND table = 't_projection_codec_old_enum' AND name = 'x'"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${table} VALUES (2, 'b')"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${table}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${table}"

# Without a projection codec, format 1 can still carry this `ALTER`.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${plain_table} (k UInt64, x Enum8('a' = 1)) ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} "${v1_safe[@]}" -q "
    ALTER TABLE ${plain_table} ON CLUSTER test_shard_localhost
        MODIFY COLUMN x ADD ENUM VALUES ('b' = 2) FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT type LIKE '%b%' FROM system.columns
    WHERE database = currentDatabase() AND table = 't_projection_codec_old_enum_plain' AND name = 'x'"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${plain_table}"
