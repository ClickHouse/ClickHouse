#!/usr/bin/env bash
# The definer's profile pins the legacy interpreter, so the view context and the caller disagree on the
# interpreter. The header of a parameterized view must follow the caller, as `StorageView::read` does.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user=user_$CLICKHOUSE_TEST_UNIQUE_NAME
profile=profile_$CLICKHOUSE_TEST_UNIQUE_NAME

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS $user"
$CLICKHOUSE_CLIENT --query "DROP SETTINGS PROFILE IF EXISTS $profile"
trap '$CLICKHOUSE_CLIENT --query "DROP USER $user"; $CLICKHOUSE_CLIENT --query "DROP SETTINGS PROFILE $profile"' EXIT

$CLICKHOUSE_CLIENT --query "CREATE SETTINGS PROFILE $profile SETTINGS allow_experimental_analyzer = 0 READONLY"
$CLICKHOUSE_CLIENT --query "CREATE USER $user SETTINGS PROFILE '$profile'"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON $CLICKHOUSE_DATABASE.* TO $user"

$CLICKHOUSE_CLIENT --enable_analyzer=1 --parallel_distributed_insert_select=2 --query "
CREATE TABLE t (id UInt8, p String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE d (c UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/d', '1') ORDER BY c;
CREATE VIEW v DEFINER = $user SQL SECURITY DEFINER AS SELECT count() AS c FROM (SELECT i.* FROM t AS i LEFT JOIN t AS a ON i.p = a.p LEFT JOIN t AS b ON i.p = b.p WHERE i.id = {id:UInt8}) AS i WHERE i.p = 'a';
INSERT INTO d SELECT * FROM v(id = 1);
SELECT * FROM d;
DROP VIEW v;
DROP TABLE d SYNC;
"
