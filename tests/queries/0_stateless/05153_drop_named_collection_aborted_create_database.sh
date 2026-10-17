#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A database engine registers its dependency on a named collection while the engine arguments are
# resolved, before the database exists. A `CREATE DATABASE` that fails after that point must not leave
# the entry behind: it is keyed by the database name only, and a database created under the same name
# later - with another engine, or with another collection - would otherwise keep the collection of the
# failed create from being dropped for good.

OLD_NC="old_nc_${CLICKHOUSE_DATABASE}"
NEW_NC="new_nc_${CLICKHOUSE_DATABASE}"
DB="${CLICKHOUSE_DATABASE}_reused"

echo "--- a CREATE DATABASE that fails after the collection was resolved ---"
# `S3` resolves the collection first and rejects the unknown key afterwards.
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${OLD_NC} AS url = 'http://localhost:1/', unexpected_key = 1;
CREATE DATABASE ${DB} ENGINE = S3(${OLD_NC}); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.databases WHERE name = '${DB}';
"

echo "--- the name is reused by a database that does not use the collection ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE DATABASE ${DB};
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${OLD_NC};
SELECT count() FROM system.named_collections WHERE name = '${OLD_NC}';
DROP DATABASE ${DB};
"

echo "--- the name is reused by a database that uses another collection ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${OLD_NC} AS url = 'http://localhost:1/', unexpected_key = 1;
CREATE NAMED COLLECTION ${NEW_NC} AS url = 'http://localhost:1/';
CREATE DATABASE ${DB} ENGINE = S3(${OLD_NC}); -- { serverError BAD_ARGUMENTS }
CREATE DATABASE ${DB} ENGINE = S3(${NEW_NC});
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${OLD_NC};
SELECT count() FROM system.named_collections WHERE name = '${OLD_NC}';
DROP NAMED COLLECTION ${NEW_NC}; -- { serverError NAMED_COLLECTION_IS_USED }
DROP DATABASE ${DB};
DROP NAMED COLLECTION ${NEW_NC};
SELECT count() FROM system.named_collections WHERE name = '${NEW_NC}';
"
