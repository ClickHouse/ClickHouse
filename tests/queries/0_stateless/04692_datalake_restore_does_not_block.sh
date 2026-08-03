#!/usr/bin/env bash
# Tags: no-fasttest

# `RESTORE DATABASE` of a `DataLakeCatalog` database goes through an internal `CREATE`, not an `ATTACH`.
# Building the catalog there would perform network I/O or credential validation, so a catalog that is
# unreachable or rejects the request would make the restore fail (and, on a replica restoring at
# startup, block it). Internal creates must build the catalog lazily instead, exactly like `ATTACH` does.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_datalake"
BACKUP="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"
# A reachable endpoint that is not a catalog: the test server's own HTTP port answers 404, which is
# not retried. A port with nothing listening would work too, but the connection error is retried
# `http_max_tries` times with exponential backoff, costing ~30 seconds per failing query.
ENDPOINT="http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/04692_not_a_catalog"

# Attached, not created, so that the catalog is not built here either.
${CLICKHOUSE_CLIENT} --query "
ATTACH DATABASE ${DB}
ENGINE = DataLakeCatalog('${ENDPOINT}')
SETTINGS catalog_type = 'rest', warehouse = 'demo';
"

${CLICKHOUSE_CLIENT} --query "BACKUP DATABASE ${DB} TO ${BACKUP}" > /dev/null
${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB}"

# The restore must succeed without touching the catalog.
${CLICKHOUSE_CLIENT} --allow_experimental_database_iceberg=1 --query "RESTORE DATABASE ${DB} FROM ${BACKUP}" > /dev/null
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.databases WHERE name = '${DB}'"

# The broken catalog is reported on first use instead.
${CLICKHOUSE_CLIENT} --query "CHECK DATABASE ${DB}" > /dev/null 2>&1 && echo "unexpectedly succeeded" || echo "reported on first use"

# A user `CREATE DATABASE` against the same endpoint still fails up front.
${CLICKHOUSE_CLIENT} --allow_experimental_database_iceberg=1 --query "
CREATE DATABASE ${DB}_create
ENGINE = DataLakeCatalog('${ENDPOINT}')
SETTINGS catalog_type = 'rest', warehouse = 'demo';
" > /dev/null 2>&1 && echo "unexpectedly succeeded" || echo "rejected on create"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}"
