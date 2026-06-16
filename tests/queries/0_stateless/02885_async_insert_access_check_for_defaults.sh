#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

restricted_user=${CLICKHOUSE_DATABASE}_restricted_user_$RANDOM$RANDOM

# The 'restricted_user' will not have access to the dictionary 'dict',
# so they shouldn't be able to insert to a table with DEFAULT dictGet(dict, ...)

$CLICKHOUSE_CLIENT <<EOF
DROP USER IF EXISTS ${restricted_user};
DROP TABLE IF EXISTS table_with_default;
DROP DICTIONARY IF EXISTS dict;
DROP TABLE IF EXISTS table_for_dict;

CREATE USER ${restricted_user};

CREATE TABLE table_for_dict (key UInt64, value String) ENGINE = Memory();

INSERT INTO table_for_dict VALUES (1, 'value_1'), (2, 'value_2'), (3, 'value_3'), (4, 'value_4'), (5, 'value_5'), (6, 'value_6');

CREATE DICTIONARY dict
(
  key UInt64,
  value String
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'table_for_dict' DB currentDatabase()))
LIFETIME(1)
LAYOUT(FLAT());

CREATE TABLE table_with_default(key UInt64, value String DEFAULT dictGetString(dict, 'value', key)) ENGINE = Memory();
GRANT INSERT, SELECT ON table_with_default TO ${restricted_user};
EOF

$CLICKHOUSE_CLIENT --query "INSERT INTO table_with_default (key) VALUES (1)"
$CLICKHOUSE_CLIENT --async_insert=1 --query "INSERT INTO table_with_default (key) VALUES (2)"

$CLICKHOUSE_CLIENT --query "SELECT * FROM table_with_default WHERE key IN [1, 2] ORDER BY key"

$CLICKHOUSE_CLIENT --user "${restricted_user}" --query "INSERT INTO table_with_default (key) VALUES (3)" 2>&1 | grep -m 1 -oF "Not enough privileges"
$CLICKHOUSE_CLIENT --user "${restricted_user}" --async_insert=1 --query "INSERT INTO table_with_default (key) VALUES (4)" 2>&1 | grep -m 1 -oF 'Not enough privileges'
$CLICKHOUSE_CLIENT --query "SELECT * FROM table_with_default WHERE key IN [3, 4] ORDER BY key"

# We give the 'restricted_user' access to the dictionary 'dict',
# so now they should be able to insert to a table with DEFAULT dictGet(dict, ...)
$CLICKHOUSE_CLIENT --query "GRANT dictGet ON dict TO ${restricted_user}"
$CLICKHOUSE_CLIENT --user "${restricted_user}" --query "INSERT INTO table_with_default (key) VALUES (5)"
$CLICKHOUSE_CLIENT --user "${restricted_user}" --async_insert=1 --query "INSERT INTO table_with_default (key) VALUES (6)"
$CLICKHOUSE_CLIENT --query "SELECT * FROM table_with_default WHERE key IN [5, 6] ORDER BY key"

$CLICKHOUSE_CLIENT <<EOF
DROP USER ${restricted_user};
DROP TABLE table_with_default;
DROP DICTIONARY dict;
DROP TABLE table_for_dict;
EOF
