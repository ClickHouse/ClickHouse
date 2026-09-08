#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: exercises the `S3Queue` engine, which is not compiled into the fast-test build.
# Tag no-replicated-database: named collections are server-global, not database-scoped
#
# `S3Queue` must honor the S3 user-credential restriction the same way the `s3` table function and `S3`
# engine do, including the per-session/profile `s3_allow_server_credentials_in_user_queries` override given
# in the CREATE statement (the storage is built with the global context, so the override has to be carried
# into it explicitly).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="$CLICKHOUSE_DATABASE"
TABLE="s3queue_creds_${DB}"
NC="s3queue_creds_nc_${DB}"

# A named collection that asks for the server's environment credentials (`use_environment_credentials = 1`
# overrides the global default). The setting is explicit so the test does not depend on the server's global
# `use_environment_credentials` value. A leftover collection is reused rather than recreated: dropping it
# is refused while a leftover table still references it.
$CLICKHOUSE_CLIENT -q "
    CREATE NAMED COLLECTION IF NOT EXISTS ${NC} AS
        url = 'http://localhost:11111/test/${DB}_q/',
        use_environment_credentials = 1
"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE} SYNC"

# Without the override the S3Queue would resolve the server's environment credentials, so it is rejected.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = S3Queue(${NC}, format = 'TSV')
    SETTINGS mode = 'ordered'
    -- { serverError ACCESS_DENIED }
"

# With the session-level override the table is created (the override reaches the S3 client built in the
# storage constructor).
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = S3Queue(${NC}, format = 'TSV')
    SETTINGS mode = 'ordered', s3_allow_server_credentials_in_user_queries = 1
"
echo "s3queue_override: created"

# Chained so the collection outlives the table: metadata must never reference a missing collection.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE} SYNC" && $CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS ${NC}"
