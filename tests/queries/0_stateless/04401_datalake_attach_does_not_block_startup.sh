#!/usr/bin/env bash
# Tags: no-fasttest

# A DataLakeCatalog database persisted by an older version (which validated auth_header lazily)
# must still attach when loaded by a newer version, so that one misconfigured or unreachable
# database cannot block server startup. The error is reported lazily on first use instead.
# Regression test for the eager auth_header validation / eager catalog construction on ATTACH.

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
