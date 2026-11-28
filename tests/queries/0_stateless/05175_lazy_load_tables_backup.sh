#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table of a database with `lazy_load_tables` is kept in the catalog as a proxy until it is loaded and
# nobody holds the proxy any more. `BACKUP` and `RESTORE` asked the proxy whether the table supports
# backing up partitions, and the proxy did not forward the question.

DB="${CLICKHOUSE_DATABASE}_lazy"
BACKUP_ALL="${CLICKHOUSE_DATABASE}_all"
BACKUP_PART="${CLICKHOUSE_DATABASE}_part"

$CLICKHOUSE_CLIENT --query "
DROP DATABASE IF EXISTS $DB SYNC;
CREATE DATABASE $DB ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE $DB.mt (a UInt64) ENGINE = MergeTree PARTITION BY a % 2 ORDER BY a;
INSERT INTO $DB.mt SELECT number FROM numbers(100);

CREATE TABLE $DB.mt2 (a UInt64) ENGINE = MergeTree PARTITION BY a % 2 ORDER BY a;
"

# Reloading the database installs the proxies, as a server restart does.
reattach() { $CLICKHOUSE_CLIENT --query "DETACH DATABASE $DB; ATTACH DATABASE $DB;"; }

reattach
echo "--- not accessed yet"
$CLICKHOUSE_CLIENT --query "SELECT engine FROM system.tables WHERE database = '$DB' AND name = 'mt'"

echo "--- backup of the whole table"
$CLICKHOUSE_CLIENT --query "
BACKUP TABLE $DB.mt TO Memory('$BACKUP_ALL') FORMAT Null;
DROP TABLE $DB.mt SYNC;
RESTORE TABLE $DB.mt FROM Memory('$BACKUP_ALL') FORMAT Null;
SELECT count() FROM $DB.mt;
"

echo "--- backup and restore of a single partition"
reattach
$CLICKHOUSE_CLIENT --query "
BACKUP TABLE $DB.mt PARTITIONS 0 TO Memory('$BACKUP_PART') FORMAT Null;
RESTORE TABLE $DB.mt AS $DB.mt2 PARTITIONS 0 FROM Memory('$BACKUP_PART') SETTINGS allow_non_empty_tables = 1 FORMAT Null;
SELECT count(), min(a % 2), max(a % 2) FROM $DB.mt2;
"

$CLICKHOUSE_CLIENT --query "DROP DATABASE $DB SYNC"
