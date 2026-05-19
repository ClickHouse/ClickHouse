#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `table_readonly` setting is not supported for `ReplicatedMergeTree`.

# A freshly generated UUID per run avoids collisions when the flaky check runs the test
# concurrently with the same hardcoded UUID.
uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

# `CREATE TABLE` should fail.
$CLICKHOUSE_CLIENT --query="
CREATE TABLE t_readonly_repl (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_readonly_repl', 'r1') ORDER BY x SETTINGS table_readonly = 1;
" 2>&1 | grep -F "NOT_IMPLEMENTED" -c

# `ATTACH TABLE` should fail too.
# The UUID-form is required because `Atomic` rejects `ATTACH TABLE name (cols) ENGINE = ...` syntax
# before reaching the storage check.
$CLICKHOUSE_CLIENT --query="
ATTACH TABLE t_readonly_repl UUID '$uuid' (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_readonly_repl', 'r1') ORDER BY x SETTINGS table_readonly = 1;
" 2>&1 | grep -F "NOT_IMPLEMENTED" -c

# `ALTER MODIFY SETTING` should fail.
$CLICKHOUSE_CLIENT --query="
CREATE TABLE t_readonly_repl (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_readonly_repl', 'r1') ORDER BY x;
"
$CLICKHOUSE_CLIENT --query="
ALTER TABLE t_readonly_repl MODIFY SETTING table_readonly = 1;
" 2>&1 | grep -F "NOT_IMPLEMENTED" -c

# A mixed `ALTER` (column + settings) must not slip the setting through the non-pure-settings path.
$CLICKHOUSE_CLIENT --query="
ALTER TABLE t_readonly_repl ADD COLUMN y UInt64, MODIFY SETTING table_readonly = 1;
" 2>&1 | grep -F "NOT_IMPLEMENTED" -c

$CLICKHOUSE_CLIENT --query="DROP TABLE t_readonly_repl"
