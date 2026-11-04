#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database, no-shared-merge-tree

# `ATTACH TABLE ... AS REPLICATED` removes the transaction metadata files (`txn_version.txt`) of the parts.
# A table in an `Ordinary` database gets such files after `RENAME TABLE` from an `Atomic` database, where
# its parts were written under an implicit transaction. Removing them while the previous, detached storage
# instance of the table is still alive fails `assertHasValidVersionMetadata` when that instance is destroyed,
# so the conversion has to wait for the detached instance the same way it does for `Atomic` tables.

# Creation of a database with Ordinary engine emits a warning.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ORDINARY_DB="ordinary_$CLICKHOUSE_DATABASE"

${CLICKHOUSE_CLIENT} --allow_deprecated_database_ordinary=1 -n -q "
    CREATE DATABASE $ORDINARY_DB ENGINE = Ordinary;
    CREATE TABLE t_txn_to_ordinary (x UInt64) ENGINE = MergeTree ORDER BY x;
"

# Insert under implicit transactions to get parts with transaction metadata.
# async_insert is disabled because it is incompatible with implicit_transaction.
${CLICKHOUSE_CLIENT} --implicit_transaction=1 --async_insert=0 -q "INSERT INTO t_txn_to_ordinary VALUES (1)"
${CLICKHOUSE_CLIENT} --implicit_transaction=1 --async_insert=0 -q "INSERT INTO t_txn_to_ordinary VALUES (2)"

${CLICKHOUSE_CLIENT} -n -q "
    RENAME TABLE t_txn_to_ordinary TO $ORDINARY_DB.t_txn_to_ordinary;

    -- The parts keep their transaction metadata after the move.
    SELECT count() FROM system.parts
    WHERE database = '$ORDINARY_DB' AND table = 't_txn_to_ordinary' AND active AND creation_tid.1 != 1;

    -- A plain ATTACH of an Ordinary table does not wait for the previous detached instance, so this leaves
    -- two detached instances of the same name behind; the conversion must wait for both of them.
    DETACH TABLE $ORDINARY_DB.t_txn_to_ordinary;
    ATTACH TABLE $ORDINARY_DB.t_txn_to_ordinary;
    DETACH TABLE $ORDINARY_DB.t_txn_to_ordinary;
    ATTACH TABLE $ORDINARY_DB.t_txn_to_ordinary AS REPLICATED;

    SELECT engine FROM system.tables WHERE database = '$ORDINARY_DB' AND name = 't_txn_to_ordinary';
    SELECT x FROM $ORDINARY_DB.t_txn_to_ordinary ORDER BY x;

    DETACH TABLE $ORDINARY_DB.t_txn_to_ordinary;
    ATTACH TABLE $ORDINARY_DB.t_txn_to_ordinary AS NOT REPLICATED;

    SELECT engine FROM system.tables WHERE database = '$ORDINARY_DB' AND name = 't_txn_to_ordinary';
    SELECT x FROM $ORDINARY_DB.t_txn_to_ordinary ORDER BY x;

    DROP TABLE $ORDINARY_DB.t_txn_to_ordinary;
    DROP DATABASE $ORDINARY_DB;
"
