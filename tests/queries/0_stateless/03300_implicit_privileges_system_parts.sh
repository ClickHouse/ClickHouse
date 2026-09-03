#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user03300_$CLICKHOUSE_DATABASE"
db=${CLICKHOUSE_DATABASE}

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS test;
DROP TABLE IF EXISTS test_1;
DROP TABLE IF EXISTS test_2;

CREATE TABLE test (s String) ENGINE = Merge(currentDatabase(), 'test_');

CREATE TABLE test_1 (s Int) ENGINE = MergeTree() ORDER BY s AS SELECT * FROM numbers(10);
CREATE TABLE test_2 (s Int) ENGINE = MergeTree() ORDER BY s AS SELECT * FROM numbers(10);
"

echo "system.parts via default"
$CLICKHOUSE_CLIENT -q "SELECT DISTINCT table FROM system.parts WHERE database = currentDatabase() AND table LIKE 'test%' ORDER BY 1"

# system.parts requires SHOW_TABLES, that is granted implicitly to user due to
# SELECT privilege
$CLICKHOUSE_CLIENT -m -q "
DROP USER IF EXISTS $user;
CREATE USER $user;

GRANT SELECT ON $db.test TO $user;
GRANT SELECT ON system.parts TO $user;
"
echo "system.parts via restricted user"
$CLICKHOUSE_CLIENT --user "$user" -q "SELECT DISTINCT table FROM system.parts WHERE database = currentDatabase() AND table LIKE 'test%' ORDER BY 1"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE test;
DROP TABLE test_1;
DROP TABLE test_2;

DROP USER $user;
"

