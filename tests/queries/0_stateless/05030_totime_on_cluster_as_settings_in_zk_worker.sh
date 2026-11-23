#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: `ON CLUSTER is not allowed for Replicated database`
# `CREATE TABLE ... AS` with `use_legacy_to_time = 1` ON CLUSTER stores exactly what a local `CREATE` stores.
# The source is created with `use_legacy_to_time = 0`, so its stored key is the raw `toTime` spelling
# and a verbatim copy is distinguishable from a rewritten one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_time_time64_type 1 --use_legacy_to_time 0 -q "
CREATE TABLE src (c0 DateTime) ENGINE = MergeTree() ORDER BY toTime(c0);
SELECT 'source', sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'src';
"

LEGACY=(--allow_experimental_time_time64_type 1 --use_legacy_to_time 1 --distributed_ddl_output_mode none)

${CLICKHOUSE_CLIENT} "${LEGACY[@]}" -q "
CREATE TABLE ${CLICKHOUSE_DATABASE}.dst_v2 ON CLUSTER test_shard_localhost AS ${CLICKHOUSE_DATABASE}.src;
SELECT 'settings_in_zk_as', sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'dst_v2';
"

${CLICKHOUSE_CLIENT} -q "
DROP TABLE dst_v2;
DROP TABLE src;
"
