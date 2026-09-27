#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A user whose own profile enables the feature can log in with DEFAULT DATABASE db.ns
# even when the server profile does not enable it.

DB=$CLICKHOUSE_DATABASE
USER="user_04614_${CLICKHOUSE_DATABASE}"
PROFILE="profile_04614_${CLICKHOUSE_DATABASE}"
CH="$CLICKHOUSE_CLIENT --allow_experimental_table_namespaces=1 --enable_analyzer=1"

$CH -m -q "
CREATE TABLE $DB.\`ns.t\` (x Int32) ENGINE = Memory;
INSERT INTO $DB.\`ns.t\` VALUES (1);
DROP USER IF EXISTS $USER;
DROP SETTINGS PROFILE IF EXISTS $PROFILE;
CREATE SETTINGS PROFILE $PROFILE SETTINGS allow_experimental_table_namespaces = 1, enable_analyzer = 1;
CREATE USER $USER IDENTIFIED WITH plaintext_password BY 'pass_04614' DEFAULT DATABASE \`$DB.ns\` SETTINGS PROFILE $PROFILE;
GRANT SELECT, SHOW ON $DB.* TO $USER;
"

echo "-- login binds the namespace scope with the user's own profile"
# the harness client pins --database, which would override the user's DEFAULT DATABASE
CLIENT_NO_DB=$(echo "$CLICKHOUSE_CLIENT" | sed 's/--database=[^ ]*//')
$CLIENT_NO_DB --user "$USER" --password 'pass_04614' -q "SELECT count() FROM t"
$CLIENT_NO_DB --user "$USER" --password 'pass_04614' -q "SELECT currentDatabase() = '$DB'"

$CH -m -q "
DROP USER $USER;
DROP SETTINGS PROFILE $PROFILE;
DROP TABLE $DB.\`ns.t\`;
"
