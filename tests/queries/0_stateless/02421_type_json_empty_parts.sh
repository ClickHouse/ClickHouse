#!/usr/bin/env bash
# Tags: no-fasttest

set -euo pipefail

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./parts.lib
. "$CURDIR"/parts.lib

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_empty_parts;"
${CLICKHOUSE_CLIENT} -q "SELECT 'Collapsing';"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_json_empty_parts (id UInt64, s Int8, data Object('json')) ENGINE = CollapsingMergeTree(s) ORDER BY id SETTINGS old_parts_lifetime=5;" --allow_experimental_object_type 1
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_json_empty_parts VALUES (1, 1, '{\"k1\": \"aaa\"}') (1, -1, '{\"k2\": \"bbb\"}');"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_json_empty_parts;"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.parts WHERE table = 't_json_empty_parts' AND database = currentDatabase() AND active;"
${CLICKHOUSE_CLIENT} -q "DESC TABLE t_json_empty_parts SETTINGS describe_extend_object_types = 1;"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_empty_parts;"
${CLICKHOUSE_CLIENT} -q "SELECT 'DELETE all';"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_json_empty_parts (id UInt64, data Object('json')) ENGINE = MergeTree ORDER BY id SETTINGS old_parts_lifetime=5;" --allow_experimental_object_type 1
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_json_empty_parts VALUES (1, '{\"k1\": \"aaa\"}') (1, '{\"k2\": \"bbb\"}');"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_json_empty_parts;"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.parts WHERE table = 't_json_empty_parts' AND database = currentDatabase() AND active;"
${CLICKHOUSE_CLIENT} -q "DESC TABLE t_json_empty_parts SETTINGS describe_extend_object_types = 1;"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_json_empty_parts DELETE WHERE 1 SETTINGS mutations_sync = 1;"
timeout 60 bash -c 'wait_for_delete_empty_parts t_json_empty_parts'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_json_empty_parts;"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.parts WHERE table = 't_json_empty_parts' AND database = currentDatabase() AND active;"
${CLICKHOUSE_CLIENT} -q "DESC TABLE t_json_empty_parts SETTINGS describe_extend_object_types = 1;"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_empty_parts;"
${CLICKHOUSE_CLIENT} -q "SELECT 'TTL';"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_json_empty_parts (id UInt64, d Date, data Object('json')) ENGINE = MergeTree ORDER BY id TTL d WHERE id % 2 = 1 SETTINGS old_parts_lifetime=5;" --allow_experimental_object_type 1
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_json_empty_parts VALUES (1, '2000-01-01', '{\"k1\": \"aaa\"}') (2, '2000-01-01', '{\"k2\": \"bbb\"}');"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_json_empty_parts FINAL;"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_json_empty_parts;"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.parts WHERE table = 't_json_empty_parts' AND database = currentDatabase() AND active;"
${CLICKHOUSE_CLIENT} -q "DESC TABLE t_json_empty_parts SETTINGS describe_extend_object_types = 1;"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_json_empty_parts MODIFY TTL d;"
${CLICKHOUSE_CLIENT} -q "OPTIMIZE TABLE t_json_empty_parts FINAL;"
timeout 60 bash -c 'wait_for_delete_empty_parts t_json_empty_parts'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_json_empty_parts;"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.parts WHERE table = 't_json_empty_parts' AND database = currentDatabase() AND active;"
${CLICKHOUSE_CLIENT} -q "DESC TABLE t_json_empty_parts SETTINGS describe_extend_object_types = 1;"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_json_empty_parts;"
