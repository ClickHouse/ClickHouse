#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Requires postgresql-client

# The PostgreSQL wire server does not decode binary input for bound parameters or `COPY`, so the
# emulated catalog must not advertise binary receive functions. Clients use `typreceive != 0` to
# choose a binary-input path, which must remain unavailable until it is implemented end-to-end.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run: the flaky check runs this test many times concurrently,
# and a global name would collide with `ACCESS_ENTITY_ALREADY_EXISTS`.
PG_USER="postgresql_user_04850_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${PG_USER};
"

function run_psql()
{
    psql --host 127.0.0.1 --port "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" --user "${PG_USER}" \
        --no-align --tuples-only --quiet -c "$1"
}

echo "--- no type advertises unsupported binary input"
run_psql "SELECT count() FROM pg_type WHERE typreceive != 0"

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER};"
