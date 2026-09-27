#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs psql

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Multipart table paths must parse on the PostgreSQL wire protocol when the
# user's settings enable the feature.

DB=$CLICKHOUSE_DATABASE
USER="pg_user_04615_${CLICKHOUSE_DATABASE}"
CH="$CLICKHOUSE_CLIENT --allow_experimental_table_namespaces=1 --enable_analyzer=1"

$CH -m -q "
CREATE TABLE $DB.\`ns.t\` (x Int32) ENGINE = Memory;
INSERT INTO $DB.\`ns.t\` VALUES (41), (1);
DROP USER IF EXISTS $USER;
CREATE USER $USER HOST IP '127.0.0.1' IDENTIFIED WITH no_password
    SETTINGS allow_experimental_table_namespaces = 1, enable_analyzer = 1;
GRANT SELECT ON $DB.* TO $USER;
"

psql --host localhost --port "${CLICKHOUSE_PORT_POSTGRESQL}" "$DB" --user "$USER" --tuples-only --no-align -c "SELECT sum(x) FROM $DB.ns.t;"

$CH -m -q "
DROP USER $USER;
DROP TABLE $DB.\`ns.t\`;
"
