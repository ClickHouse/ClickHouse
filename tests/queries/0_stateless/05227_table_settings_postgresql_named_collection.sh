#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: depends on libpqxx (the `PostgreSQL` table engine), which is not built in fast test.
#
# The `PostgreSQL` engine resolves its settings from three places in turn: the creating session, a named
# collection, and the table's own `SETTINGS` clause. The table keeps the settings and the collection's name, so
# all three are told apart - and a value the collection supplied is reported as such even when it happens to be
# the compiled-in default, which no comparison of values could reveal.
#
# The engine does not connect at `CREATE` time when the columns are given explicitly, so an unreachable host is
# fine here. A shell test because a named collection is server-wide, so its name has to carry this test's database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

NC="pg_${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT -q "
SET send_logs_level = 'fatal';
DROP TABLE IF EXISTS pg_collection;
DROP NAMED COLLECTION IF EXISTS ${NC};

-- 16 is also the compiled-in default of \`postgresql_connection_pool_size\`, so a report derived from the value
-- alone would call it \`default\`; 4 differs from the default of \`postgresql_connection_pool_retries\`.
CREATE NAMED COLLECTION ${NC} AS
    host = 'localhost', port = 5432, database = 'db', table = 'tbl', user = 'u', password = 'p',
    postgresql_connection_pool_size = 16, postgresql_connection_pool_retries = 4;

SET postgresql_connection_attempt_timeout = 7;

CREATE TABLE pg_collection (x Int32) ENGINE = PostgreSQL(${NC})
    SETTINGS postgresql_connection_pool_wait_timeout = 3000;

SELECT '-- the collection, the clause, the session and the default are all told apart';
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'pg_collection'
ORDER BY name;

DROP TABLE pg_collection;
DROP NAMED COLLECTION ${NC};
"
