#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/78628
#
# The custom key filtering for the parallel replicas was requested without the custom key, and the
# query failed with `Empty query`: the empty value of `parallel_replicas_custom_key` was parsed as if
# it were a query, and the syntax error had nothing in it to point at the setting. A missing key is
# reported as a missing key now, and a key that holds no expression at all - only whitespace or a
# comment - is reported by a syntax error that names the text it failed on.
#
# `serialize_query_plan = 0`: the custom-key parallel replicas reject `serialize_query_plan`, and the
# CI `distributed plan` shard turns it on globally.

$CLICKHOUSE_CLIENT -q "CREATE TABLE t_05037 (c0 Int) ENGINE = MergeTree ORDER BY tuple()"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_05037 VALUES (1)"

PR_SETTINGS="enable_parallel_replicas = 1, max_parallel_replicas = 3, automatic_parallel_replicas_mode = 0,
    serialize_query_plan = 0, parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost'"

MISSING_KEY="The custom key filtering for the parallel replicas is requested (setting 'parallel_replicas_mode'), but the custom key itself is not set (setting 'parallel_replicas_custom_key')"

# The custom key is not set at all - both queries from the issue. The whole message is asserted: it
# has to name `parallel_replicas_mode` as the setting that asks for the custom key filtering, and
# `parallel_replicas_custom_key` as the setting that is missing.
#
# `enable_analyzer = 1` on the first one: the old interpreter turns the parallel replicas off for a
# `JOIN` before it looks at the custom key, so the query just runs.
$CLICKHOUSE_CLIENT -q "SELECT 1 FROM t_05037 JOIN t_05037 AS t1 ON t_05037.c0 = t1.c0
    SETTINGS $PR_SETTINGS, enable_analyzer = 1, parallel_replicas_mode = 'custom_key_sampling'" 2>&1 |
    grep -o -m1 "$MISSING_KEY"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_05037 WHERE c0 > 0
    SETTINGS $PR_SETTINGS, parallel_replicas_mode = 'custom_key_range'" 2>&1 |
    grep -o -m1 "$MISSING_KEY"

# The custom key is set to a text that holds no expression.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_05037
    SETTINGS $PR_SETTINGS, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = ' '" 2>&1 |
    grep -o -m1 "Empty query (parallel replicas custom key)"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_05037
    SETTINGS $PR_SETTINGS, parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = '-- the key'" 2>&1 |
    grep -o -m1 "Empty query (parallel replicas custom key)"

# Every other text that is parsed on its own is named the same way.
#
# `enable_analyzer = 1`: the old interpreter parses `additional_result_filter` under the name
# `additional filter`, the same name it gives to `additional_table_filters`.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_05037 SETTINGS enable_analyzer = 1, additional_result_filter = ' '" 2>&1 |
    grep -o -m1 "Empty query (additional result filter)"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_05037 SETTINGS additional_table_filters = {'t_05037': ' '}" 2>&1 |
    grep -o -m1 "Empty query (additional filter)"

# The custom key that parses is still used to split the work between the replicas.
$CLICKHOUSE_CLIENT -q "SELECT c0 FROM t_05037 ORDER BY c0
    SETTINGS $PR_SETTINGS, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'c0'"
