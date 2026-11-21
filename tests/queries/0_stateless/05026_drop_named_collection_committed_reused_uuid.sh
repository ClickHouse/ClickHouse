#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-replicated-database: explicit UUIDs are forbidden there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A dependency of a table of an `Atomic` database is resolved by its UUID, because `RENAME` changes the
# name recorded in the entry while the table keeps using the collection. `CREATE TABLE ... UUID` can
# reuse the UUID of a failed create under a different name, though, and the table that owns the UUID
# then has nothing to do with the collection of the failed create: it must not keep that collection
# alive.

OLD_NC="old_nc_${CLICKHOUSE_DATABASE}"
NEW_NC="new_nc_${CLICKHOUSE_DATABASE}"

uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")

echo "--- a failed CREATE TABLE ... UUID leaves a stale dependency ---"
# The collection resolves while the engine arguments are resolved (the dependency is registered), and
# the storage constructor then rejects the unknown format, so the create fails.
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${OLD_NC} AS url = 'http://localhost:8123', format = 'ThisFormatDoesNotExist';
CREATE NAMED COLLECTION ${NEW_NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE old_t UUID '${uuid}' (x UInt32) ENGINE = URL(${OLD_NC}); -- { serverError UNKNOWN_FORMAT }
CREATE TABLE new_t UUID '${uuid}' (x UInt32) ENGINE = URL(${NEW_NC});
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
DROP TABLE new_t;
DROP NAMED COLLECTION ${NEW_NC};
SELECT count() FROM system.named_collections WHERE name = '${NEW_NC}';
"
