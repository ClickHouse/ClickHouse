#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TRINO_OPTS="--allow_experimental_trino_dialect 1 --dialect trino"

# The trino dialect requires the experimental setting
$CLICKHOUSE_CLIENT --dialect trino -q "SELECT 1" 2>&1 | grep -om1 "SUPPORT_IS_DISABLED"

# SET queries work even when the feature gate is off, so a misconfigured profile is recoverable
$CLICKHOUSE_CLIENT --dialect trino -q "SET dialect = 'clickhouse'" && echo "SET works without the gate"

# Unsupported constructs report clear errors
$CLICKHOUSE_CLIENT $TRINO_OPTS -q "SELECT count() FROM numbers(10) TABLESAMPLE BERNOULLI (10)" 2>&1 | grep -om1 "NOT_IMPLEMENTED"
$CLICKHOUSE_CLIENT $TRINO_OPTS -q "SELECT TRY(1 / 0)" 2>&1 | grep -om1 "NOT_IMPLEMENTED"
$CLICKHOUSE_CLIENT $TRINO_OPTS -q "SELECT 1 FROM (SELECT ARRAY[1] AS a) CROSS JOIN UNNEST(a)" 2>&1 | grep -om1 "NOT_IMPLEMENTED"
$CLICKHOUSE_CLIENT $TRINO_OPTS -q "SELECT split('a,b', ',', 2)" 2>&1 | grep -om1 "BAD_ARGUMENTS"
$CLICKHOUSE_CLIENT $TRINO_OPTS -q "SELECT approx_percentile(number, number / 10) FROM numbers(10)" 2>&1 | grep -om1 "BAD_ARGUMENTS"
$CLICKHOUSE_CLIENT $TRINO_OPTS -q "SELECT json_size('{}', concat('$', '.a'))" 2>&1 | grep -om1 "NOT_IMPLEMENTED"

# Multi-statement scripts are split correctly
$CLICKHOUSE_CLIENT $TRINO_OPTS -q "SELECT ARRAY[1, 2]; SELECT cardinality(ARRAY['a']);"

# The translation is observable through EXPLAIN SYNTAX
$CLICKHOUSE_CLIENT $TRINO_OPTS -q "EXPLAIN SYNTAX SELECT approx_distinct(x) FROM (VALUES 1, 2) AS t(x)"
