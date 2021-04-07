#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "
DROP USER IF EXISTS postgresql_protocol_user;
DROP TABLE IF EXISTS postgresql_protocol_with_row_policy;
DROP ROW POLICY IF EXISTS test_policy ON postgresql_protocol_with_row_policy;

CREATE TABLE postgresql_protocol_with_row_policy (val UInt32) ENGINE=MergeTree ORDER BY val;
INSERT INTO postgresql_protocol_with_row_policy SELECT number FROM numbers(10);

SELECT 'before row policy';
SELECT * FROM postgresql_protocol_with_row_policy;
SELECT '';
" | $CLICKHOUSE_CLIENT -n

echo "
CREATE USER postgresql_protocol_user HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT(val) ON postgresql_protocol_with_row_policy TO postgresql_protocol_user;
CREATE ROW POLICY IF NOT EXISTS test_policy ON postgresql_protocol_with_row_policy FOR SELECT USING val = 2 TO postgresql_protocol_user;

SELECT 'after row policy with no password';
" | $CLICKHOUSE_CLIENT -n

psql --host localhost --port 9005 default --user postgresql_protocol_user -c "SELECT * FROM postgresql_protocol_with_row_policy;"

echo "
DROP USER IF EXISTS postgresql_protocol_user;
DROP ROW POLICY IF EXISTS test_policy ON postgresql_protocol_with_row_policy;
CREATE USER postgresql_protocol_user HOST IP '127.0.0.1' IDENTIFIED WITH plaintext_password BY 'qwerty';
GRANT SELECT(val) ON postgresql_protocol_with_row_policy TO postgresql_protocol_user;
CREATE ROW POLICY IF NOT EXISTS test_policy ON postgresql_protocol_with_row_policy FOR SELECT USING val = 2 TO postgresql_protocol_user;

SELECT 'after row policy with plaintext_password';
" | $CLICKHOUSE_CLIENT -n

psql "postgresql://postgresql_protocol_user:qwerty@localhost:9005/default" -c "SELECT * FROM postgresql_protocol_with_row_policy;"

