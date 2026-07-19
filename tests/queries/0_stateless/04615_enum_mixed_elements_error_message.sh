#!/usr/bin/env bash
# Test for issue #93691: the error message for a mix of implicitly and explicitly
# valued Enum elements described a per-element form that every rejected element
# already satisfied. It now describes the actual constraint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An unsupported mix must mention the actual rule.
$CLICKHOUSE_CLIENT -q "CREATE TEMPORARY TABLE t04615_bad (e Enum('a' = 1, 'b', 'c' = 3))" 2>&1 | grep -c "must be of the same form"

# The supported shapes still work.
$CLICKHOUSE_CLIENT -q "CREATE TEMPORARY TABLE t04615_implicit (e Enum('a', 'b', 'c'))" && echo ok
$CLICKHOUSE_CLIENT -q "CREATE TEMPORARY TABLE t04615_explicit (e Enum('a' = 1, 'b' = 2))" && echo ok
$CLICKHOUSE_CLIENT -q "CREATE TEMPORARY TABLE t04615_explicit_head (e Enum('a' = 10, 'b', 'c'))" && echo ok
$CLICKHOUSE_CLIENT -q "CREATE TEMPORARY TABLE t04615_implicit_head (e Enum('a', 'b' = 5, 'c' = 6))" && echo ok
