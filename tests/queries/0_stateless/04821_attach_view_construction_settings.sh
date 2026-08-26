#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Query-construction settings (`select` / `filter` / `order` / `sort` / `limit` / `offset` / `page`)
# are rejected in a fresh CREATE of a view definition. But `limit` and `offset` are pre-existing
# setting names, so a view created by an older server can legitimately hold `SETTINGS limit = ...`
# in its stored metadata. Loading that metadata (server startup, upgrade, ATTACH) must keep working
# instead of failing the whole database load.
#
# `clickhouse-local --path` persists table metadata, so a second invocation on the same path goes
# through the real `DatabaseOnDisk` metadata-load (ATTACH) path.

DATA_DIR="${CLICKHOUSE_TMP}/04821_data"
rm -rf "${DATA_DIR}"

echo "-- create a view without construction settings"
${CLICKHOUSE_LOCAL} --path "${DATA_DIR}" -q "
CREATE DATABASE db;
CREATE VIEW db.v_old AS SELECT number AS x FROM numbers(10);
" < /dev/null

echo "-- inject a construction setting into the stored metadata, as an older server could have written it"
SQL_FILE=$(find "${DATA_DIR}" -name 'v_old.sql' | head -1)
sed -i "s/FROM numbers(10)/FROM numbers(10) SETTINGS limit = 3/" "${SQL_FILE}"
grep -c "SETTINGS limit = 3" "${SQL_FILE}"

echo "-- metadata load (ATTACH) accepts the definition; the view is readable"
${CLICKHOUSE_LOCAL} --path "${DATA_DIR}" -q "SELECT count() FROM db.v_old" < /dev/null

echo "-- a DETACH / ATTACH round-trip also works"
${CLICKHOUSE_LOCAL} --path "${DATA_DIR}" -q "
DETACH TABLE db.v_old;
ATTACH TABLE db.v_old;
SELECT count() FROM db.v_old;
" < /dev/null

echo "-- a fresh CREATE is still rejected"
${CLICKHOUSE_LOCAL} --path "${DATA_DIR}" -q "CREATE VIEW db.v_new AS SELECT number AS x FROM numbers(10) SETTINGS limit = 3" < /dev/null 2>&1 | grep -oE "NOT_IMPLEMENTED" | head -1

rm -rf "${DATA_DIR}"
