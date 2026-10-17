#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-replicated-database: explicit UUIDs are forbidden there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Like `05026_drop_named_collection_committed_reused_uuid`, but the database is renamed before the drop:
# `RENAME DATABASE` must re-key the dependencies of the tables of the database as well, otherwise the
# entries of the committed table keep the old database name, and the table is taken for the owner of the
# stale entry of the failed create that reused its UUID.

OLD_NC="old_nc_${CLICKHOUSE_DATABASE}"
NEW_NC="new_nc_${CLICKHOUSE_DATABASE}"
DB1="${CLICKHOUSE_DATABASE}_1"
DB2="${CLICKHOUSE_DATABASE}_2"

uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")

echo "--- a failed CREATE TABLE ... UUID leaves a stale dependency ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE DATABASE ${DB1} ENGINE = Atomic;
CREATE NAMED COLLECTION ${OLD_NC} AS url = 'http://localhost:8123', format = 'ThisFormatDoesNotExist';
CREATE NAMED COLLECTION ${NEW_NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE ${DB1}.old_t UUID '${uuid}' (x UInt32) ENGINE = URL(${OLD_NC}); -- { serverError UNKNOWN_FORMAT }
CREATE TABLE ${DB1}.new_t UUID '${uuid}' (x UInt32) ENGINE = URL(${NEW_NC});
RENAME DATABASE ${DB1} TO ${DB2};
"

echo "--- the committed table does not keep the collection of the failed create alive ---"
${CLICKHOUSE_CLIENT} -m -q "
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${OLD_NC};
SELECT count() FROM system.named_collections WHERE name = '${OLD_NC}';
"

echo "--- but it does keep its own collection alive ---"
${CLICKHOUSE_CLIENT} -m -q "
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${NEW_NC}; -- { serverError NAMED_COLLECTION_IS_USED }
DROP DATABASE ${DB2};
DROP NAMED COLLECTION ${NEW_NC};
SELECT count() FROM system.named_collections WHERE name = '${NEW_NC}';
"
