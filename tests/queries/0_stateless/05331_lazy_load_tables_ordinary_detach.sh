#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The database tables iterator hands out the storage behind a materialized lazy-load stand-in, so an
# `Ordinary` database has to track that storage, not the stand-in, to know whether a detached table is
# still in use. `DETACH` and `ATTACH` of such a table must keep working.

DB="${CLICKHOUSE_DATABASE}_lazy_ordinary"

$CLICKHOUSE_CLIENT --allow_deprecated_database_ordinary 1 --send_logs_level error --query "
DROP DATABASE IF EXISTS $DB SYNC;
CREATE DATABASE $DB ENGINE = Ordinary SETTINGS lazy_load_tables = 1;
CREATE TABLE $DB.mt (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO $DB.mt SELECT number FROM numbers(100);
"

# Reloading the database installs the stand-in, as a server restart does.
$CLICKHOUSE_CLIENT --send_logs_level error --query "DETACH DATABASE $DB; ATTACH DATABASE $DB;"

$CLICKHOUSE_CLIENT --send_logs_level error --query "
SELECT count() FROM $DB.mt;
SELECT count() FROM system.parts WHERE database = '$DB' AND table = 'mt' AND active;
DETACH TABLE $DB.mt;
ATTACH TABLE $DB.mt;
SELECT count() FROM $DB.mt;
DETACH TABLE $DB.mt;
CREATE TABLE $DB.mt (a UInt64) ENGINE = MergeTree ORDER BY a; -- { serverError TABLE_ALREADY_EXISTS }
ATTACH TABLE $DB.mt;
SELECT count() FROM $DB.mt;
"

$CLICKHOUSE_CLIENT --send_logs_level error --query "DROP DATABASE $DB SYNC"
