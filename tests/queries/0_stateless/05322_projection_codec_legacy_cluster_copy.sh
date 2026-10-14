#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dst_legacy ON CLUSTER test_shard_localhost FORMAT Null;
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dst_non_merge ON CLUSTER test_shard_localhost FORMAT Null;
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dst_current ON CLUSTER test_shard_localhost FORMAT Null;
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.src;
CREATE TABLE ${CLICKHOUSE_DATABASE}.src
    (k UInt64, v UInt64, PROJECTION p (v CODEC(ZSTD)) AS (SELECT k, v ORDER BY k))
ENGINE = MergeTree ORDER BY k;
"

# A legacy entry copies its source on each worker; that worker rejects a codec projection
# before publishing its destination.
if codec_copy_error=$(${CLICKHOUSE_CLIENT} \
    --allow_projection_column_codecs_in_replicated_or_distributed_ddl 1 \
    --distributed_ddl_entry_format_version 2 --distributed_ddl_output_mode throw -q "
CREATE TABLE ${CLICKHOUSE_DATABASE}.dst_legacy ON CLUSTER test_shard_localhost
AS ${CLICKHOUSE_DATABASE}.src FORMAT Null;
" 2>&1); then
    printf 'legacy copy unexpectedly succeeded\n' >&2
    exit 1
fi
if [[ ${codec_copy_error} != *SUPPORT_IS_DISABLED* ]]; then
    printf '%s\n' "${codec_copy_error}" >&2
    exit 1
fi
printf 'legacy_rejected\n'

${CLICKHOUSE_CLIENT} -q "
SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name = 'dst_legacy';
SELECT count() = 1 FROM system.distributed_ddl_queue
WHERE startsWith(query, 'CREATE TABLE ') AND query LIKE '%${CLICKHOUSE_DATABASE}%dst_legacy%';
"

# A non-MergeTree destination does not copy projection metadata and remains valid at format 2.
${CLICKHOUSE_CLIENT} --allow_projection_column_codecs_in_replicated_or_distributed_ddl 1 \
    --distributed_ddl_entry_format_version 2 --distributed_ddl_output_mode throw -q "
CREATE TABLE ${CLICKHOUSE_DATABASE}.dst_non_merge ON CLUSTER test_shard_localhost
AS ${CLICKHOUSE_DATABASE}.src ENGINE = Memory FORMAT Null;
"
${CLICKHOUSE_CLIENT} -q "
SELECT count() = 0 FROM system.projections
WHERE database = currentDatabase() AND table = 'dst_non_merge';
DROP TABLE dst_non_merge ON CLUSTER test_shard_localhost FORMAT Null;
"

# A current entry copies and normalizes the source on the initiator before enqueueing.
${CLICKHOUSE_CLIENT} --allow_projection_column_codecs_in_replicated_or_distributed_ddl 1 \
    --distributed_ddl_entry_format_version 5 --distributed_ddl_output_mode throw -q "
CREATE TABLE ${CLICKHOUSE_DATABASE}.dst_current ON CLUSTER test_shard_localhost
AS ${CLICKHOUSE_DATABASE}.src FORMAT Null;
"
${CLICKHOUSE_CLIENT} -q "
SELECT count() = 1 FROM system.projections
WHERE database = currentDatabase() AND table = 'dst_current' AND name = 'p';
INSERT INTO dst_current VALUES (1, 2);
SELECT sum(v) = 2 FROM dst_current;
DROP TABLE dst_current ON CLUSTER test_shard_localhost FORMAT Null;
DROP TABLE src;
"
