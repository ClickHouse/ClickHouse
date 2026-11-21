#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-replicated-database: explicit UUIDs are forbidden there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A failed `CREATE TABLE ... UUID` leaves a stale dependency under its name, and its UUID can be
# reused by a later create under a different name. The stale entry is told apart from the live
# table's own entries by the recorded name, so the names of the entries must follow the tables
# across `EXCHANGE TABLES` as well: otherwise, after the live table is exchanged into the name of the
# failed create, the stale entry would be taken for its own, and the collection the live table uses
# could be dropped.

OLD_NC="old_nc_${CLICKHOUSE_DATABASE}"
NEW_NC="new_nc_${CLICKHOUSE_DATABASE}"

uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")

echo "--- a failed CREATE TABLE ... UUID leaves a stale dependency, the UUID is reused, the tables are exchanged ---"
# The collection resolves while the engine arguments are resolved (the dependency is registered), and
# the storage constructor then rejects the unknown format, so the create fails.
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${OLD_NC} AS url = 'http://localhost:8123', format = 'ThisFormatDoesNotExist';
CREATE NAMED COLLECTION ${NEW_NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE b UUID '${uuid}' (x UInt32) ENGINE = URL(${OLD_NC}); -- { serverError UNKNOWN_FORMAT }
CREATE TABLE a UUID '${uuid}' (x UInt32) ENGINE = URL(${NEW_NC});
CREATE TABLE b (x UInt32) ENGINE = Memory;
EXCHANGE TABLES a AND b;
"

echo "--- the exchanged table keeps its own collection alive ---"
${CLICKHOUSE_CLIENT} -m -q "
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${NEW_NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"

echo "--- also when it is detached ---"
${CLICKHOUSE_CLIENT} -m -q "
SET check_named_collection_dependencies = true;
DETACH TABLE b;
DROP NAMED COLLECTION ${NEW_NC}; -- { serverError NAMED_COLLECTION_IS_USED }
ATTACH TABLE b;
"

echo "--- but not the collection of the failed create ---"
${CLICKHOUSE_CLIENT} -m -q "
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${OLD_NC};
SELECT count() FROM system.named_collections WHERE name = '${OLD_NC}';
"

echo "--- after the table is dropped, its collection can be dropped ---"
${CLICKHOUSE_CLIENT} -m -q "
SET check_named_collection_dependencies = true;
DROP TABLE a;
DROP TABLE b;
DROP NAMED COLLECTION ${NEW_NC};
SELECT count() FROM system.named_collections WHERE name = '${NEW_NC}';
"
