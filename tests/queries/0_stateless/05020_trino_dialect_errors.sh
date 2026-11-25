#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TRINO_OPTS="--enable_trino_dialect 1 --dialect trino"

# The trino dialect requires the experimental setting
$CLICKHOUSE_CLIENT --dialect trino -q "SELECT 1" 2>&1 | grep -om1 "SUPPORT_IS_DISABLED"
# The message must name the setting to turn on; assert the name only, not the surrounding prose
$CLICKHOUSE_CLIENT --dialect trino -q "SELECT 1" 2>&1 | grep -om1 "enable_trino_dialect"

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

# The input() initializer in clickhouse-local reparses the original query, and must reparse with the
# gate the query was accepted with: a query-local `SETTINGS enable_trino_dialect = 0` applies during
# execution, after the parse that accepted the query.
printf '1\n' | ${CLICKHOUSE_LOCAL} --enable_trino_dialect 1 --dialect trino \
    -q "INSERT INTO FUNCTION null('x UInt8') SELECT x FROM input('x UInt8') SETTINGS enable_trino_dialect = 0 FORMAT TSV" \
    && echo 'local input gate ok'

# Without the gate the same query is rejected, so the assertion above is keyed on the gate.
printf '1\n' | ${CLICKHOUSE_LOCAL} --dialect trino \
    -q "INSERT INTO FUNCTION null('x UInt8') SELECT x FROM input('x UInt8') FORMAT TSV" 2>&1 | grep -om1 "SUPPORT_IS_DISABLED"
