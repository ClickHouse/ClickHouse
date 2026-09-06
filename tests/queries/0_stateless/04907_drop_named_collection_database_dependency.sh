#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A database engine can reference a named collection as well, and its `CREATE DATABASE` metadata is
# replayed at the next server start just like the metadata of a table, so dropping the collection
# under it would keep the server from starting.

NC="nc_${CLICKHOUSE_DATABASE}"
DB="${CLICKHOUSE_DATABASE}_s3"

echo "--- a database using the collection holds it ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:1/';
CREATE DATABASE ${DB} ENGINE = S3(${NC});
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"

echo "--- a detached database holds it, too: its metadata is still replayed ---"
${CLICKHOUSE_CLIENT} -m -q "
DETACH DATABASE ${DB};
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
"

echo "--- dropping the database releases it ---"
${CLICKHOUSE_CLIENT} -m -q "
ATTACH DATABASE ${DB};
DROP DATABASE ${DB};
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"

echo "--- a database that uses no collection is detached and dropped as usual ---"
${CLICKHOUSE_CLIENT} -m -q "
CREATE DATABASE ${DB};
DETACH DATABASE ${DB};
ATTACH DATABASE ${DB};
DROP DATABASE ${DB};
SELECT count() FROM system.databases WHERE name = '${DB}';
"
