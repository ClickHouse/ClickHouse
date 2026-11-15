#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database, no-shared-merge-tree, no-encrypted-storage

# Regression test: ATTACH TABLE AS REPLICATED with implicit_transaction caused
# assertHasValidVersionMetadata() LOGICAL_ERROR in debug/sanitizer builds.
# The implicit transaction creates parts with txn_version.txt on disk.
# clearTransactionMetadata deleted those files before the old detached table
# was destroyed, so the old parts' destructors failed the version metadata check.

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

${CLICKHOUSE_CLIENT} -n -q "
    DETACH TABLE t_implicit_txn;
    ATTACH TABLE t_implicit_txn AS REPLICATED;

    SELECT x FROM t_implicit_txn ORDER BY x;

    DETACH TABLE t_implicit_txn;
    ATTACH TABLE t_implicit_txn AS NOT REPLICATED;

    DROP TABLE t_implicit_txn;
"
