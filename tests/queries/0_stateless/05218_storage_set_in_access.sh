#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
db=${CLICKHOUSE_DATABASE}

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS set_table;
DROP TABLE IF EXISTS mt_table;
CREATE TABLE set_table (n Int) ENGINE = Set;
INSERT INTO set_table VALUES (4242), (31337);
CREATE TABLE mt_table (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO mt_table VALUES (4242), (31337);

DROP USER IF EXISTS $user;
CREATE USER $user IDENTIFIED WITH no_password;
"

# Without any grant, the set contents are readable neither directly nor through the right of IN.
$CLICKHOUSE_CLIENT --user "$user" -m -q "SELECT * FROM set_table; -- { serverError ACCESS_DENIED }"
$CLICKHOUSE_CLIENT --user "$user" -m -q "SELECT number FROM numbers(100000) WHERE number IN set_table; -- { serverError ACCESS_DENIED }"
# An ordinary table on the right of IN requires SELECT; the set table must behave the same.
$CLICKHOUSE_CLIENT --user "$user" -m -q "SELECT number FROM numbers(100000) WHERE number IN mt_table; -- { serverError ACCESS_DENIED }"

# INSERT is the grant one gets to add to a blocklist without being allowed to read it.
$CLICKHOUSE_CLIENT -m -q "GRANT INSERT ON $db.set_table TO $user"

$CLICKHOUSE_CLIENT --user "$user" -m -q "SELECT number FROM numbers(100000) WHERE number IN set_table; -- { serverError ACCESS_DENIED }"
# A query that declares itself secondary keeps enable_analyzer = 0, i.e. the old analysis path.
$CLICKHOUSE_CLIENT --user "$user" --query_kind secondary_query --enable_analyzer 0 -m -q "SELECT number FROM numbers(100000) WHERE number IN set_table; -- { serverError ACCESS_DENIED }"

# A TTL WHERE is analyzed in the context of the user running the INSERT. DDL does not read the set,
# so the CREATE below succeeds and the denial lands on the INSERT.
$CLICKHOUSE_CLIENT -m -q "GRANT CREATE TABLE, INSERT ON $db.ttl_table TO $user"
$CLICKHOUSE_CLIENT --user "$user" -m -q "
CREATE TABLE ttl_table (x UInt64, d DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY x
    TTL d + INTERVAL 1 SECOND DELETE WHERE x IN $db.set_table;
"
$CLICKHOUSE_CLIENT --user "$user" -m -q "INSERT INTO ttl_table (x) SELECT 1; -- { serverError ACCESS_DENIED }"

$CLICKHOUSE_CLIENT -m -q "GRANT SELECT ON $db.set_table TO $user"

$CLICKHOUSE_CLIENT --user "$user" -m -q "SELECT number FROM numbers(100000) WHERE number IN set_table"
$CLICKHOUSE_CLIENT --user "$user" --query_kind secondary_query --enable_analyzer 0 -m -q "SELECT number FROM numbers(100000) WHERE number IN set_table"
$CLICKHOUSE_CLIENT --user "$user" -m -q "INSERT INTO ttl_table (x) SELECT 1"

# The check is column-level, so a grant covering every column of the set table is enough.
$CLICKHOUSE_CLIENT -m -q "REVOKE SELECT ON $db.set_table FROM $user; GRANT SELECT(n) ON $db.set_table TO $user"
$CLICKHOUSE_CLIENT --user "$user" -m -q "SELECT number FROM numbers(100000) WHERE number IN set_table"

$CLICKHOUSE_CLIENT -m -q "DROP USER $user"
