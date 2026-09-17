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

CREATE NAMED COLLECTION ${NC} AS
    host = 'localhost', port = 3306, database = 'db', table = 'tbl', user = 'u', password = 'p',
    connection_pool_size = 16, connect_timeout = 7;

CREATE TABLE mysql_collection (x Int32) ENGINE = MySQL(${NC})
    SETTINGS read_write_timeout = 1234;

SELECT '-- the collection, the clause and the default are told apart';
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'mysql_collection'
  AND name IN ('connection_pool_size', 'connect_timeout', 'read_write_timeout')
ORDER BY name;

DROP TABLE mysql_collection;
DROP NAMED COLLECTION ${NC};
"
