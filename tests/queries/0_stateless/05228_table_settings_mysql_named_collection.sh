#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: depends on the MySQL table engine, which is an optional build.
#
# `MySQL` resolves its settings from the creating session, a named collection and the table's own `SETTINGS`
# clause, in that order. It keeps the collection's name, so a value the collection supplied is reported as
# such - which no comparison of values could reveal, since a collection may well state the default.
#
# The engine does not connect at `CREATE` time when the columns are given explicitly. A shell test because a
# named collection is server-wide, so its name has to carry this test's database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

NC="my_${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT -q "
SET send_logs_level = 'fatal';
DROP TABLE IF EXISTS mysql_collection;
DROP NAMED COLLECTION IF EXISTS ${NC};

-- \`connection_wait_timeout\` is stated at its own compiled-in default, so nothing but the reported
-- source can say the collection is where it came from.
CREATE NAMED COLLECTION ${NC} AS
    host = 'localhost', port = 3306, database = 'db', table = 'tbl', user = 'u', password = 'p',
    connection_pool_size = 16, connect_timeout = 7, connection_wait_timeout = 5;

-- \`connect_timeout\` is stated by both the collection and the clause, and \`connection_pool_size\` is
-- overridden in the engine arguments - the two cases where naming the collection would be wrong.
CREATE TABLE mysql_collection (x Int32) ENGINE = MySQL(${NC}, connection_pool_size = 42)
    SETTINGS read_write_timeout = 1234, connect_timeout = 99;

SELECT '-- the collection, the clause, an engine-argument override and the default are told apart';
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'mysql_collection'
  AND name IN ('connection_pool_size', 'connect_timeout', 'read_write_timeout', 'connection_wait_timeout')
ORDER BY name;
"

# Unconditionally, so that a failure above cannot leave a server-wide collection behind for later tests.
$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS mysql_collection;
DROP NAMED COLLECTION IF EXISTS ${NC};
"
