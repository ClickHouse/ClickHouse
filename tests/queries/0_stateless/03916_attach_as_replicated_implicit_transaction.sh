#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database, no-shared-merge-tree

# Regression test: ATTACH TABLE AS REPLICATED used to remove the txn_version.txt files that an
# implicit transaction writes on its parts, which failed assertHasValidVersionMetadata() in the old
# detached table's destructor in debug and sanitizer builds. The conversion refuses instead.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE t_implicit_txn (x UInt64) ENGINE = MergeTree ORDER BY x;
"

# Insert with implicit_transaction to create parts with transaction metadata
# Disable async_insert because it is incompatible with implicit_transaction
${CLICKHOUSE_CLIENT} --implicit_transaction=1 --async_insert=0 -q "INSERT INTO t_implicit_txn VALUES (1)"
${CLICKHOUSE_CLIENT} --implicit_transaction=1 --async_insert=0 -q "INSERT INTO t_implicit_txn VALUES (2)"

${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_implicit_txn"
${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null -q "ATTACH TABLE t_implicit_txn AS REPLICATED" 2>&1 |
    grep -c 'transactions were used on this table'
${CLICKHOUSE_CLIENT} -n -q "
    ATTACH TABLE t_implicit_txn;

    SELECT x FROM t_implicit_txn ORDER BY x;

    DROP TABLE t_implicit_txn;
"
