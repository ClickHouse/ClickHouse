#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: exercises the `S3Queue` engine, which is not compiled into the fast-test build.
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

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE} SYNC"

# Without the override: a URL-only S3Queue would resolve the server's environment credentials, so it is
# rejected.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = S3Queue('http://localhost:11111/test/${DB}_q/', 'TSV')
    SETTINGS mode = 'ordered'
    -- { serverError ACCESS_DENIED }
"

# With the session-level override the table is created (the override reaches the S3 client built in the
# storage constructor; the request goes unsigned because no explicit credentials are given).
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = S3Queue('http://localhost:11111/test/${DB}_q/', 'TSV')
    SETTINGS mode = 'ordered', s3_allow_server_credentials_in_user_queries = 1
"
echo "s3queue_override: created"

$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${TABLE} SYNC"
