#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree
# Tag no-replicated-database, no-shared-merge-tree: custom disk

# `CREATE DATABASE` / `ATTACH DATABASE` accept a SETTINGS clause without an explicit ENGINE, the same
# way `CREATE TABLE` does, so that an inline `disk(...)` can be used the way a single table uses it.
# See https://github.com/ClickHouse/ClickHouse/issues/118409

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_DISK="disk(type = local, path = '${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_DATABASE}_metadata/')"

${CLICKHOUSE_CLIENT} --multiline -q "
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_inline SYNC;
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_engine_settings SYNC;
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_query_settings SYNC;
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_unknown SYNC;
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_memory SYNC;
"

echo '-- inline disk without an explicit ENGINE'
${CLICKHOUSE_CLIENT} --multiline -q "
CREATE DATABASE ${CLICKHOUSE_DATABASE}_inline SETTINGS disk = ${DB_DISK};
CREATE TABLE ${CLICKHOUSE_DATABASE}_inline.t (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO ${CLICKHOUSE_DATABASE}_inline.t VALUES (42);
SELECT * FROM ${CLICKHOUSE_DATABASE}_inline.t;
"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE DATABASE ${CLICKHOUSE_DATABASE}_inline" --format TSVRaw

echo '-- the inline disk was really created'
${CLICKHOUSE_CLIENT} -q "
SELECT count() FROM system.disks WHERE path = '${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_DATABASE}_metadata/'
"

# The database metadata files live on that disk and nowhere else, so re-attaching the database with the
# same inline disk is what brings its tables back.
echo '-- ATTACH DATABASE with the same inline disk brings the tables back'
DB_UUID=$(${CLICKHOUSE_CLIENT} -q "SELECT uuid FROM system.databases WHERE name = '${CLICKHOUSE_DATABASE}_inline'")
${CLICKHOUSE_CLIENT} --multiline -q "
DETACH DATABASE ${CLICKHOUSE_DATABASE}_inline;
ATTACH DATABASE ${CLICKHOUSE_DATABASE}_inline UUID '${DB_UUID}' SETTINGS disk = ${DB_DISK};
SELECT * FROM ${CLICKHOUSE_DATABASE}_inline.t;
"

echo '-- a non-disk database setting works without an explicit ENGINE too'
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_engine_settings SETTINGS lazy_load_tables = 1"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE DATABASE ${CLICKHOUSE_DATABASE}_engine_settings" --format TSVRaw

echo '-- engine settings and query settings can be mixed in one clause'
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${CLICKHOUSE_DATABASE}_engine_settings SYNC"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_engine_settings ENGINE = Atomic SETTINGS lazy_load_tables = 1, max_threads = 4"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE DATABASE ${CLICKHOUSE_DATABASE}_engine_settings" --format TSVRaw

echo '-- a clause holding only query settings leaves no SETTINGS behind'
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_query_settings SETTINGS max_threads = 4"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE DATABASE ${CLICKHOUSE_DATABASE}_query_settings" --format TSVRaw

echo '-- the short ATTACH syntax still reads the definition from the metadata file'
${CLICKHOUSE_CLIENT} --multiline -q "
DETACH DATABASE ${CLICKHOUSE_DATABASE}_query_settings;
ATTACH DATABASE ${CLICKHOUSE_DATABASE}_query_settings SETTINGS max_threads = 4;
"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE DATABASE ${CLICKHOUSE_DATABASE}_query_settings" --format TSVRaw

echo '-- an unknown setting is still reported as such'
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_unknown SETTINGS not_a_setting_at_all = 1" 2>&1 | grep -om1 "UNKNOWN_SETTING"

echo '-- a database engine without settings support rejects them'
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_memory ENGINE = Memory SETTINGS lazy_load_tables = 1" 2>&1 | grep -om1 "UNKNOWN_ELEMENT_IN_AST"

${CLICKHOUSE_CLIENT} --multiline -q "
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_inline SYNC;
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_engine_settings SYNC;
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_query_settings SYNC;
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_unknown SYNC;
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_memory SYNC;
"
