#!/usr/bin/env bash
# Tags: no-fasttest

# A DataLakeCatalog database persisted by an older version (which validated auth_header lazily)
# must still attach when loaded by a newer version, so that one misconfigured or unreachable
# database cannot block server startup. The error is reported lazily on first use instead.
# Regression test for the eager auth_header validation / eager catalog construction on ATTACH.

# The server prints the rejected-header exception to its log and (at least in CI) forwards Error
# log messages to the client, which would duplicate the expected "is forbidden" line. Suppress it.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKDIR="${CLICKHOUSE_TMP}/04401_datalake_attach"
rm -rf "${WORKDIR}"
mkdir -p "${WORKDIR}/metadata"

# Database with a malformed auth_header (no colon), as an older version would have persisted it.
cat > "${WORKDIR}/metadata/baddb.sql" <<'EOF'
ATTACH DATABASE baddb
ENGINE = DataLakeCatalog('http://localhost:8181/v1')
SETTINGS catalog_type = 'rest', auth_header = 'malformed_without_colon', warehouse = 'demo'
EOF

# Database with a well-formed auth_header but an unreachable endpoint.
cat > "${WORKDIR}/metadata/netdb.sql" <<'EOF'
ATTACH DATABASE netdb
ENGINE = DataLakeCatalog('http://localhost:18181/v1')
SETTINGS catalog_type = 'rest', auth_header = 'Authorization: Bearer xyz', warehouse = 'demo'
EOF

# Loading the data dir re-attaches every database, exactly as server startup does.
# Both databases must attach successfully (no validation, no network I/O on attach).
${CLICKHOUSE_LOCAL} --path "${WORKDIR}" --query "SELECT name FROM system.databases WHERE name IN ('baddb', 'netdb') ORDER BY name"

# First use of the malformed database must still report the error (lazy validation).
${CLICKHOUSE_LOCAL} --path "${WORKDIR}" --query "CHECK DATABASE baddb" 2>&1 | grep -o 'Unexpected format of auth header'

# CREATE with a malformed auth_header must still be rejected up front.
${CLICKHOUSE_LOCAL} --query "
SET allow_experimental_database_iceberg = 1;
CREATE DATABASE baddb_create
ENGINE = DataLakeCatalog('http://localhost:8181/v1')
SETTINGS catalog_type = 'rest', auth_header = 'malformed_without_colon', warehouse = 'demo';
" 2>&1 | grep -o "Invalid auth header format"

rm -rf "${WORKDIR}"

# A forbidden auth_header (http_forbid_headers) persisted by an older version must also attach
# without blocking startup, but it must still be rejected on first use so the forbidden header
# is never sent to the catalog. http_forbid_headers is read only by the server (not by
# clickhouse-local), and 'exact_header' is forbidden via tests/config/config.d/forbidden_headers.xml,
# so this part runs against the test server.
FORBIDDEN_DB="${CLICKHOUSE_DATABASE}_04401_forbidden"
${CLICKHOUSE_CLIENT} --query "
ATTACH DATABASE ${FORBIDDEN_DB}
ENGINE = DataLakeCatalog('http://localhost:18181/v1')
SETTINGS catalog_type = 'rest', auth_header = 'exact_header: some_value', warehouse = 'demo';
"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.databases WHERE name = '${FORBIDDEN_DB}'"
${CLICKHOUSE_CLIENT} --query "CHECK DATABASE ${FORBIDDEN_DB}" 2>&1 | grep -o 'is forbidden'
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${FORBIDDEN_DB}"
