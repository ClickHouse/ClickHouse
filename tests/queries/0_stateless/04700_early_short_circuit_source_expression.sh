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

# The same literal is visible in the projection name, in a folded concat result, and as concat
# input, while its use as the encryption key is hidden.
if grep -qF '[HIDDEN' <<< "$secret_output" \
    && [[ $(grep -oF 'SEKRIT_KEY_12345' <<< "$secret_output" | wc -l) -eq 3 ]]; then
    echo "secret_mask_ok"
else
    echo "secret_mask_failed"
fi

# `ignore` opts out of lazy short-circuit execution. It must remain in the resolved tree instead
# of being replaced by the analyzer-time fold.
non_lazy_output=$($CLICKHOUSE_CLIENT -q "
    EXPLAIN QUERY TREE
    SELECT 1 OR ignore(sleep(1))
    SETTINGS enable_function_early_short_circuit = 1
")

if grep -qF 'function_name: ignore, function_type: ordinary, result_type:' <<< "$non_lazy_output"; then
    echo "non_lazy_function_ok"
else
    echo "non_lazy_function_failed"
fi

$CLICKHOUSE_CLIENT -q "DROP TABLE t_04700_early_short_circuit_source"
