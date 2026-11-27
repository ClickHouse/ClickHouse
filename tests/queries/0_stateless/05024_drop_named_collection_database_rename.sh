#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-replicated-database: `RENAME DATABASE` is not supported there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `RENAME DATABASE` rewrites the `CREATE DATABASE` metadata under the new name, and that metadata keeps
# referencing the named collection, so the dependency of the database has to follow the rename.
# `MaterializedPostgreSQL` is the named-collection-backed database engine that supports the rename; it
# connects to PostgreSQL asynchronously, so the database can be created without a server to connect to.

NC="nc_${CLICKHOUSE_DATABASE}"
DB="${CLICKHOUSE_DATABASE}_pg"

${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS host = 'localhost', port = 1, database = 'db', user = 'u', password = 'p';
SET allow_experimental_database_materialized_postgresql = 1;
CREATE DATABASE ${DB} ENGINE = MaterializedPostgreSQL(${NC});
"

echo "--- the renamed database still holds the collection ---"
${CLICKHOUSE_CLIENT} -m -q "
RENAME DATABASE ${DB} TO ${DB}_renamed;
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"

echo "--- the collection is released once the database is dropped ---"
# The engine complains that it cannot drop its replication slot on a server it cannot reach.
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB}_renamed" 2>/dev/null
${CLICKHOUSE_CLIENT} -m -q "
SET check_named_collection_dependencies = true;
DROP NAMED COLLECTION ${NC};
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"
