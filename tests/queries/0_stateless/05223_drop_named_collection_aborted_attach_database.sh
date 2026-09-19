#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-parallel: the `attach_database_fail_after_load` failpoint is process-global and would fail the
# `ATTACH DATABASE` of any concurrently running test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An `ATTACH DATABASE` of a detached database that fails after the engine resolved its collection and
# after its tables were loaded rolls the live dependencies back: the database is detached again and
# the entries registered during the attach are dropped or pruned as stale. The metadata of the
# database and of its tables still references the collections, and the next `ATTACH DATABASE` or
# server start replays it, so the drop has to stay refused - the entries `DETACH DATABASE` recorded
# for the database and its tables take care of that.

DB_NC="db_nc_${CLICKHOUSE_DATABASE}"
TABLE_NC="table_nc_${CLICKHOUSE_DATABASE}"
S3_DB="${CLICKHOUSE_DATABASE}_s3"
ATOMIC_DB="${CLICKHOUSE_DATABASE}_atomic"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "SYSTEM DISABLE FAILPOINT attach_database_fail_after_load" 2>/dev/null ||:
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${DB_NC} AS url = 'http://localhost:1/';
CREATE NAMED COLLECTION ${TABLE_NC} AS url = 'http://localhost:1/', format = 'CSV';
CREATE DATABASE ${S3_DB} ENGINE = S3(${DB_NC});
CREATE DATABASE ${ATOMIC_DB} ENGINE = Atomic;
CREATE TABLE ${ATOMIC_DB}.t (x UInt8) ENGINE = URL(${TABLE_NC});
DETACH DATABASE ${S3_DB};
DETACH DATABASE ${ATOMIC_DB};
"

echo "--- the attach of the database using the collection fails after the engine resolved it ---"
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT attach_database_fail_after_load"
${CLICKHOUSE_CLIENT} -q "ATTACH DATABASE ${S3_DB}" 2>&1 | grep -o -F "FAULT_INJECTED" | head -n 1
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.databases WHERE name = '${S3_DB}'"

echo "--- the attach of the database whose table uses the collection fails after the table was loaded ---"
${CLICKHOUSE_CLIENT} -q "SYSTEM ENABLE FAILPOINT attach_database_fail_after_load"
${CLICKHOUSE_CLIENT} -q "ATTACH DATABASE ${ATOMIC_DB}" 2>&1 | grep -o -F "FAULT_INJECTED" | head -n 1
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.databases WHERE name = '${ATOMIC_DB}'"

echo "--- both collections are still held by the metadata that will be attached back ---"
${CLICKHOUSE_CLIENT} -m -q "
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${DB_NC}; -- { serverError NAMED_COLLECTION_IS_USED }
DROP NAMED COLLECTION ${TABLE_NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"

echo "--- the databases attach back and still hold them ---"
${CLICKHOUSE_CLIENT} -m -q "
ATTACH DATABASE ${S3_DB};
ATTACH DATABASE ${ATOMIC_DB};
SELECT count() FROM system.databases WHERE name IN ('${S3_DB}', '${ATOMIC_DB}');
SELECT count() FROM system.tables WHERE database = '${ATOMIC_DB}' AND name = 't';
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${DB_NC}; -- { serverError NAMED_COLLECTION_IS_USED }
DROP NAMED COLLECTION ${TABLE_NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"

echo "--- the collections are released once the databases are dropped ---"
${CLICKHOUSE_CLIENT} -m -q "
DROP DATABASE ${S3_DB};
DROP DATABASE ${ATOMIC_DB};
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${DB_NC};
DROP NAMED COLLECTION ${TABLE_NC};
SELECT count() FROM system.named_collections WHERE name IN ('${DB_NC}', '${TABLE_NC}');
"
