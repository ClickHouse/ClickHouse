#!/usr/bin/env bash
# Tags: no-parallel
# `SYSTEM STOP MERGES` without a table name is server-wide, so this test cannot share the server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The action locks are keyed by the storage object, and a table of a database with `lazy_load_tables`
# can be addressed either as the stand-in the catalog holds (`SYSTEM ... MERGES <db>.<table>`) or as
# the storage behind it (server-wide `SYSTEM ... MERGES`, which goes through the database iterator).
# Both spellings have to address the same entry, otherwise a lock taken under one of them survives a
# `SYSTEM START` under the other and merges stay blocked.

DB="${CLICKHOUSE_DATABASE}_lazy"

$CLICKHOUSE_CLIENT --query "
DROP DATABASE IF EXISTS $DB SYNC;
CREATE DATABASE $DB ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE $DB.mt (a UInt64) ENGINE = MergeTree ORDER BY a;
"

# Reloading the database installs the stand-in, as a server restart does; the `SELECT` materializes the
# real storage behind it, which is when the two spellings start to disagree.
$CLICKHOUSE_CLIENT --query "DETACH DATABASE $DB; ATTACH DATABASE $DB;"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM $DB.mt"

# The parts are inserted while the merges are already stopped, so no background merge can race with the
# `OPTIMIZE` below and turn the refusal we expect into a no-op.
echo "--- stopped server-wide, started for the single table"
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES"
$CLICKHOUSE_CLIENT --query "INSERT INTO $DB.mt SELECT number FROM numbers(10)"
$CLICKHOUSE_CLIENT --query "INSERT INTO $DB.mt SELECT number FROM numbers(10, 10)"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $DB.mt FINAL" 2>&1 | grep -o "Cancelled merging parts" | head -n 1
$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES $DB.mt"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $DB.mt FINAL"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = '$DB' AND table = 'mt' AND active"
$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES"

echo "--- stopped for the single table, started server-wide"
$CLICKHOUSE_CLIENT --query "SYSTEM STOP MERGES $DB.mt"
$CLICKHOUSE_CLIENT --query "INSERT INTO $DB.mt SELECT number FROM numbers(20, 10)"
$CLICKHOUSE_CLIENT --query "INSERT INTO $DB.mt SELECT number FROM numbers(30, 10)"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $DB.mt FINAL" 2>&1 | grep -o "Cancelled merging parts" | head -n 1
$CLICKHOUSE_CLIENT --query "SYSTEM START MERGES"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $DB.mt FINAL"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = '$DB' AND table = 'mt' AND active"

$CLICKHOUSE_CLIENT --query "DROP DATABASE $DB SYNC"
