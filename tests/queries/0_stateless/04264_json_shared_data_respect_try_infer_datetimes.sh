#!/usr/bin/env bash
# Tags: no-fasttest
# Test that insertIntoSharedData and insertIntoDynamicPath respect
# input_format_try_infer_datetimes setting for DateTime64 inference.
# https://github.com/ClickHouse/ClickHouse/issues/103221

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "SET enable_json_type = 1"

# Case 1: shared data path (max_dynamic_paths=0).
# With try_infer_datetimes=0, datetime strings must be stored as String.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_shared"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_shared (data JSON(max_dynamic_paths=0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS enable_json_type=1"

echo '{"data": {"ts": "2024-01-15 12:30:45"}}' | ${CLICKHOUSE_CLIENT} --input_format_try_infer_dates=1 --input_format_try_infer_datetimes=0 -q "INSERT INTO t_shared FORMAT JSONEachRow"

${CLICKHOUSE_CLIENT} -q "SELECT dynamicType(data.ts) FROM t_shared"
${CLICKHOUSE_CLIENT} -q "SELECT data.ts FROM t_shared"

# Verify that dates are still inferred when try_infer_dates=1.
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t_shared"
echo '{"data": {"d": "2024-01-15"}}' | ${CLICKHOUSE_CLIENT} --input_format_try_infer_dates=1 --input_format_try_infer_datetimes=0 -q "INSERT INTO t_shared FORMAT JSONEachRow"

${CLICKHOUSE_CLIENT} -q "SELECT dynamicType(data.d) FROM t_shared"

# Verify that DateTime64 is inferred when try_infer_datetimes=1.
${CLICKHOUSE_CLIENT} -q "TRUNCATE TABLE t_shared"
echo '{"data": {"ts": "2024-01-15 12:30:45"}}' | ${CLICKHOUSE_CLIENT} --input_format_try_infer_dates=1 --input_format_try_infer_datetimes=1 -q "INSERT INTO t_shared FORMAT JSONEachRow"

${CLICKHOUSE_CLIENT} -q "SELECT dynamicType(data.ts) FROM t_shared"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_shared"

# Case 2: dynamic path (max_dynamic_paths=1).
# Same behavior expected: try_infer_datetimes=0 should prevent DateTime64 inference.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_dynamic"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_dynamic (data JSON(max_dynamic_paths=1)) ENGINE = MergeTree ORDER BY tuple() SETTINGS enable_json_type=1"

echo '{"data": {"ts": "2024-01-15 12:30:45"}}' | ${CLICKHOUSE_CLIENT} --input_format_try_infer_dates=1 --input_format_try_infer_datetimes=0 -q "INSERT INTO t_dynamic FORMAT JSONEachRow"

${CLICKHOUSE_CLIENT} -q "SELECT dynamicType(data.ts) FROM t_dynamic"
${CLICKHOUSE_CLIENT} -q "SELECT data.ts FROM t_dynamic"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_dynamic"
