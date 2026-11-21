#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A bare numeric value is accepted into an `IPv4` column by the `Values` format: a value the strict
# quoted-text path of `ValuesBlockInputFormat::tryReadValue` rejects is retried as an expression, and
# the literal is then converted to the destination type like `CAST` does, which succeeds for the
# `UInt32`-backed `IPv4`. So the schema-mismatch diagnostic must not blame such a column for an
# unrelated parse error in another column. This does not depend on
# `input_format_values_interpret_expressions`: the retry first goes through
# `ConstantExpressionTemplate`, which converts a literal on its own regardless of that setting.
#
# `UUID` and `IPv6` still require a (quoted) string in every format, so a numeric value there stays a
# genuine structure mismatch. `input_format_values_interpret_expressions` is not randomized by the
# test harness, so the explicit setting below needs no `--allow_repeated_settings`.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- Values: a numeric value for an IPv4 column is valid, only the UUID is bad (no false positive)"
$CLICKHOUSE_LOCAL --query "CREATE TABLE t (ip IPv4, u UUID) ENGINE = Memory; INSERT INTO t FORMAT Values (1, 'not-a-uuid');" < /dev/null 2>&1 | check

echo "-- Values: the same holds with input_format_values_interpret_expressions = 0"
$CLICKHOUSE_LOCAL --input_format_values_interpret_expressions 0 --query "CREATE TABLE t (ip IPv4, u UUID) ENGINE = Memory; INSERT INTO t FORMAT Values (1, 'not-a-uuid');" < /dev/null 2>&1 | check

echo "-- Values: a numeric value for an IPv6 column is a genuine structure mismatch"
$CLICKHOUSE_LOCAL --query "CREATE TABLE t (u UUID, ip IPv6) ENGINE = Memory; INSERT INTO t FORMAT Values ('not-a-uuid', 1);" < /dev/null 2>&1 | check
