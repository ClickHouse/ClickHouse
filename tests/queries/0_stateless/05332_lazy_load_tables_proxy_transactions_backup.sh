#!/usr/bin/env bash
# Tags: zookeeper, no-ordinary-database, no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table of a database with `lazy_load_tables` is kept in the catalog as a stand-in. The transaction
# support of the table and the adjustment of the `CREATE TABLE` query for `BACKUP` are asked of that
# stand-in before it is materialized, so they have to see through it.

DB="${CLICKHOUSE_DATABASE}_lazy"
BACKUP_NAME="${CLICKHOUSE_DATABASE}_rmt"

$CLICKHOUSE_CLIENT --query "
DROP DATABASE IF EXISTS $DB SYNC;
CREATE DATABASE $DB ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE $DB.mt (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO $DB.mt SELECT number FROM numbers(10);

CREATE TABLE $DB.rmt (a UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/rmt', 'r1') ORDER BY a;
INSERT INTO $DB.rmt SELECT number FROM numbers(10);
"

# Reloading the database installs the stand-in, as a server restart does.
reattach() { $CLICKHOUSE_CLIENT --query "DETACH DATABASE $DB; ATTACH DATABASE $DB;"; }

echo "--- DELETE inside a transaction"
reattach
$CLICKHOUSE_CLIENT --query "
BEGIN TRANSACTION;
DELETE FROM $DB.mt WHERE a = 1;
COMMIT;
SELECT count() FROM $DB.mt;
"

echo "--- INSERT inside a transaction"
reattach
$CLICKHOUSE_CLIENT --query "
BEGIN TRANSACTION;
INSERT INTO $DB.mt SETTINGS async_insert = 0 VALUES (100);
COMMIT;
SELECT count() FROM $DB.mt;
"

echo "--- backup of a replicated table"
reattach
$CLICKHOUSE_CLIENT --query "
BACKUP TABLE $DB.rmt TO Memory('$BACKUP_NAME') FORMAT Null;
DROP TABLE $DB.rmt SYNC;
RESTORE TABLE $DB.rmt FROM Memory('$BACKUP_NAME') FORMAT Null;
SELECT count() FROM $DB.rmt;
"

$CLICKHOUSE_CLIENT --query "DROP DATABASE $DB SYNC"
