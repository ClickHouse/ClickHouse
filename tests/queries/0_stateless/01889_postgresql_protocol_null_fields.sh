#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "
DROP USER IF EXISTS postgresql_user;
CREATE USER postgresql_user HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
" | $CLICKHOUSE_CLIENT -n

psql --host localhost --port ${CLICKHOUSE_PORT_POSTGRESQL} ${CLICKHOUSE_DATABASE} --user postgresql_user -c "SELECT NULL;"
