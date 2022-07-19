#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: Creates database and users
# Tag no-fasttest: Requires postgresql-client

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "
CREATE DATABASE IF NOT EXISTS db01802;
DROP TABLE IF EXISTS db01802.postgresql;
DROP ROW POLICY IF EXISTS test_policy ON db01802.postgresql;

CREATE TABLE db01802.postgresql (val UInt32) ENGINE=MergeTree ORDER BY val;
INSERT INTO db01802.postgresql SELECT number FROM numbers(10);

SELECT 'before row policy';
SELECT * FROM db01802.postgresql;
" | $CLICKHOUSE_CLIENT -n


echo "
DROP USER IF EXISTS postgresql_user;
CREATE USER postgresql_user HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT(val) ON db01802.postgresql TO postgresql_user;
CREATE ROW POLICY IF NOT EXISTS test_policy ON db01802.postgresql FOR SELECT USING val = 2 TO postgresql_user;

SELECT '';
SELECT 'after row policy with no password';
" | $CLICKHOUSE_CLIENT -n

psql --host localhost --port ${CLICKHOUSE_PORT_POSTGRESQL} db01802 --user postgresql_user -c "SELECT * FROM postgresql;"

echo "
DROP USER IF EXISTS postgresql_user;
DROP ROW POLICY IF EXISTS test_policy ON db01802.postgresql;
CREATE USER postgresql_user HOST IP '127.0.0.1' IDENTIFIED WITH plaintext_password BY 'qwerty';
GRANT SELECT(val) ON db01802.postgresql TO postgresql_user;
CREATE ROW POLICY IF NOT EXISTS test_policy ON db01802.postgresql FOR SELECT USING val = 2 TO postgresql_user;

SELECT 'after row policy with plaintext_password';
" | $CLICKHOUSE_CLIENT -n

psql "postgresql://postgresql_user:qwerty@localhost:${CLICKHOUSE_PORT_POSTGRESQL}/db01802" -c "SELECT * FROM postgresql;"

$CLICKHOUSE_CLIENT -q "DROP TABLE db01802.postgresql"
$CLICKHOUSE_CLIENT -q "DROP DATABASE db01802"
