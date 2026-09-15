#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table of a database with `lazy_load_tables` is kept in the catalog as a stand-in that materializes
# the real storage on first access, and the catalog keeps the stand-in afterwards. Everything that reads
# the storage object itself - the `system.parts` family and the `SYSTEM` commands (which recognize an
# engine by downcasting), the `DELETE` and mutation capability predicates, and `BACKUP` - used to see
# the stand-in instead of the table.

DB="${CLICKHOUSE_DATABASE}_lazy"
BACKUP_ALL="${CLICKHOUSE_DATABASE}_all"
BACKUP_PART="${CLICKHOUSE_DATABASE}_part"

$CLICKHOUSE_CLIENT --query "
DROP DATABASE IF EXISTS $DB SYNC;
CREATE DATABASE $DB ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE $DB.mt (a UInt64) ENGINE = MergeTree PARTITION BY a % 2 ORDER BY a;
INSERT INTO $DB.mt SELECT number FROM numbers(100);

CREATE TABLE $DB.mt2 (a UInt64) ENGINE = MergeTree PARTITION BY a % 2 ORDER BY a;

CREATE TABLE $DB.mtp (a UInt64, b UInt64, PROJECTION p (SELECT b, count() GROUP BY b))
    ENGINE = MergeTree ORDER BY a;
INSERT INTO $DB.mtp SELECT number, number % 3 FROM numbers(100);
"

# Reloading the database installs the stand-in, as a server restart does.
reattach() { $CLICKHOUSE_CLIENT --query "DETACH DATABASE $DB; ATTACH DATABASE $DB;"; }

reattach
echo "--- not accessed yet"
$CLICKHOUSE_CLIENT --query "SELECT engine FROM system.tables WHERE database = '$DB' AND name = 'mt'"

echo "--- backup of the whole table"
# `BACKUP` of a table that has not been accessed since must not lose its data.
$CLICKHOUSE_CLIENT --query "
BACKUP TABLE $DB.mt TO Memory('$BACKUP_ALL') FORMAT Null;
DROP TABLE $DB.mt SYNC;
RESTORE TABLE $DB.mt FROM Memory('$BACKUP_ALL') FORMAT Null;
SELECT count() FROM $DB.mt;
"

echo "--- backup and restore of a single partition"
# `supportsBackupPartition` is answered before the data methods are reached, so it has to see through
# the stand-in as well - on both the backup and the restore side.
reattach
$CLICKHOUSE_CLIENT --query "
BACKUP TABLE $DB.mt PARTITIONS 0 TO Memory('$BACKUP_PART') FORMAT Null;
RESTORE TABLE $DB.mt AS $DB.mt2 PARTITIONS 0 FROM Memory('$BACKUP_PART') SETTINGS allow_non_empty_tables = 1 FORMAT Null;
SELECT count(), min(a % 2), max(a % 2) FROM $DB.mt2;
"

echo "--- system tables see a materialized table"
reattach
$CLICKHOUSE_CLIENT --query "
SELECT count() FROM $DB.mt;
SELECT engine FROM system.tables WHERE database = '$DB' AND name = 'mt';
SELECT count() FROM system.parts WHERE database = '$DB' AND table = 'mt' AND active;
SELECT count() FROM system.parts_columns WHERE database = '$DB' AND table = 'mt' AND active AND column = 'a';
"

echo "--- DELETE and mutations are accepted"
$CLICKHOUSE_CLIENT --query "
DELETE FROM $DB.mt WHERE a = 1;
ALTER TABLE $DB.mt DELETE WHERE a = 2 SETTINGS mutations_sync = 2;
SELECT count() FROM $DB.mt;
"

echo "--- DELETE respects lightweight_mutation_projection_mode"
# The guard resolves the mode by downcasting the storage, so it has to see through the stand-in too -
# otherwise the `THROW` mode silently degrades to dropping the projections.
$CLICKHOUSE_CLIENT --query "DELETE FROM $DB.mtp WHERE a = 1" 2>&1 | grep -o "lightweight_mutation_projection_mode is set to THROW" | head -n 1
# The projection is still there: the query was refused, not executed with the projection dropped.
$CLICKHOUSE_CLIENT --query "SELECT count() > 0 FROM system.projection_parts WHERE database = '$DB' AND table = 'mtp' AND active AND name = 'p'"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM $DB.mtp"

$CLICKHOUSE_CLIENT --query "DROP DATABASE $DB SYNC"
