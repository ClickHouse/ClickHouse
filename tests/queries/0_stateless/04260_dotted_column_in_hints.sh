#!/usr/bin/env bash
# Regression test for #90731.
# ColumnsDescription::getAllRegisteredNames must include columns whose names
# contain a dot, so they appear in IHints suggestions after a typo.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_dotted_hint (\`a.b\` Array(String))
    ENGINE = MergeTree ORDER BY tuple();
"

# Misspell the column name; the error message must suggest the real name 'a.b'.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_dotted_hint MODIFY COLUMN a_b Array(String);" 2>&1 \
    | grep -qF "a.b" && echo "ok" || echo "FAIL"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_dotted_hint;"
