#!/usr/bin/env bash
# Server-side inlined-data INSERT path: the rows stay in the query text instead of being
# pushed as a separate native block. `--send_table_structure_on_insert_with_inline_data=0`
# forces this path with clickhouse-client; it is also the path HTTP insert bodies take.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -o errexit

CLIENT="$CLICKHOUSE_CLIENT --async_insert=0 --send_table_structure_on_insert_with_inline_data=0"

$CLIENT --query "DROP TABLE IF EXISTS t_insert_returning_inline"
$CLIENT --query "CREATE TABLE t_insert_returning_inline (id UInt64, name String) ENGINE = Memory"

# Successful RETURNING result over the inlined-data path.
echo 'inline returning result'
$CLIENT --query "INSERT INTO t_insert_returning_inline (id, name) RETURNING (SELECT id, name FROM t_insert_returning_inline WHERE id = 1 ORDER BY id) VALUES (1, 'foo')"

# A RETURNING planning exception (unknown column) must still leave the inserted row in place:
# the INSERT runs first, then the subquery fails. Proves "insert first, then fail" for inlined data.
echo 'inline returning planning error'
$CLIENT --query "INSERT INTO t_insert_returning_inline (id, name) RETURNING (SELECT no_such_column FROM t_insert_returning_inline) VALUES (2, 'bar')" 2>&1 | grep -o -m1 'UNKNOWN_IDENTIFIER'

echo 'rows after failed returning'
$CLIENT --query "SELECT id, name FROM t_insert_returning_inline ORDER BY id"

$CLIENT --query "DROP TABLE t_insert_returning_inline"
