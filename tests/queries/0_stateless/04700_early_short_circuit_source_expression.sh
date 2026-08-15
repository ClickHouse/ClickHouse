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
    SELECT 1 OR notEmpty(concat('SEKRIT_KEY_12345', encrypt('aes-128-ecb', 'x', 'SEKRIT_KEY_12345')))
    SETTINGS enable_function_early_short_circuit = 1,
             format_display_secrets_in_show_and_select = 0
")

# The same literal is visible once as concat input and secret once as the encryption key.
# Position-based mask propagation must preserve the former and hide only the latter.
if grep -qF '[HIDDEN' <<< "$secret_output" \
    && [[ $(grep -oF 'SEKRIT_KEY_12345' <<< "$secret_output" | wc -l) -eq 1 ]]; then
    echo "secret_mask_ok"
else
    echo "secret_mask_failed"
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04700_early_short_circuit_source"
