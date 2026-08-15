#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: encrypt is not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_04700_early_short_circuit_source"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_04700_early_short_circuit_source (x UInt8) ENGINE = Memory"

syntax_output=$($CLICKHOUSE_CLIENT -q "
    EXPLAIN SYNTAX
    SELECT 1 OR ((SELECT count(*) FROM t_04700_early_short_circuit_source) > 0)
    SETTINGS enable_function_early_short_circuit = 1
")

if grep -qF 't_04700_early_short_circuit_source' <<< "$syntax_output" \
    && ! grep -qF '__early_short_circuit_scalar' <<< "$syntax_output"; then
    echo "syntax_source_ok"
else
    echo "syntax_source_failed"
fi

secret_output=$($CLICKHOUSE_CLIENT -q "
    EXPLAIN QUERY TREE
    SELECT 1 OR notEmpty(encrypt('aes-128-ecb', 'x', 'SEKRIT_KEY_12345'))
    SETTINGS enable_function_early_short_circuit = 1,
             format_display_secrets_in_show_and_select = 0
")

if grep -qF '[HIDDEN' <<< "$secret_output" \
    && ! grep -qF 'SEKRIT_KEY_12345' <<< "$secret_output"; then
    echo "secret_mask_ok"
else
    echo "secret_mask_failed"
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04700_early_short_circuit_source"
