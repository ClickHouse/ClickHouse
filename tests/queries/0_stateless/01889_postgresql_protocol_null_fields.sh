#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs psql

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "
DROP USER IF EXISTS postgresql_user_01889;
CREATE USER postgresql_user_01889 HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
" | $CLICKHOUSE_CLIENT

psql --host localhost --port ${CLICKHOUSE_PORT_POSTGRESQL} ${CLICKHOUSE_DATABASE} --user postgresql_user_01889 -c "SELECT NULL;"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS postgresql_user_01889"
