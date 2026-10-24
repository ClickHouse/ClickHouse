#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-replicated-database: explicit UUIDs are forbidden there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `CREATE TABLE ... UUID` that fails after its engine resolved a named collection must not leave the
# dependency behind: a retry under the same name and UUID with another collection would otherwise be
# taken for a user of the old collection, refusing its drop while no metadata references it.

OLD_NC="old_nc_${CLICKHOUSE_DATABASE}"
NEW_NC="new_nc_${CLICKHOUSE_DATABASE}"

uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")

${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${OLD_NC} AS url = 'http://localhost:8123', format = 'ThisFormatDoesNotExist';
CREATE NAMED COLLECTION ${NEW_NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE t UUID '${uuid}' (x UInt32) ENGINE = URL(${OLD_NC}); -- { serverError UNKNOWN_FORMAT }
CREATE TABLE t UUID '${uuid}' (x UInt32) ENGINE = URL(${NEW_NC});
DROP NAMED COLLECTION ${OLD_NC};
SELECT count() FROM system.named_collections WHERE name = '${OLD_NC}';
DROP NAMED COLLECTION ${NEW_NC}; -- { serverError NAMED_COLLECTION_IS_USED }
DETACH TABLE t;
DROP NAMED COLLECTION ${NEW_NC}; -- { serverError NAMED_COLLECTION_IS_USED }
ATTACH TABLE t;
DROP TABLE t;
DROP NAMED COLLECTION ${NEW_NC};
SELECT count() FROM system.named_collections WHERE name = '${NEW_NC}';
"
