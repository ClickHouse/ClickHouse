#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
db=${CLICKHOUSE_DATABASE}

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS set_table;
DROP TABLE IF EXISTS set_pair;
DROP TABLE IF EXISTS set_pair_b;
DROP TABLE IF EXISTS set_mat;
DROP TABLE IF EXISTS set_rp;
DROP TABLE IF EXISTS mt_table;
DROP TABLE IF EXISTS ttl_table;
DROP TABLE IF EXISTS proj_table;
CREATE TABLE set_table (n Int) ENGINE = Set;
INSERT INTO set_table VALUES (4242), (31337);
CREATE TABLE set_pair (a UInt64, b UInt64) ENGINE = Set;
INSERT INTO set_pair VALUES (4242, 1), (31337, 1);
CREATE TABLE set_pair_b (a UInt64, b UInt64) ENGINE = Set;
INSERT INTO set_pair_b VALUES (4242, 1), (31337, 1);
CREATE TABLE set_mat (a UInt64, m UInt64 MATERIALIZED a + 1) ENGINE = Set;
INSERT INTO set_mat VALUES (4242);
CREATE TABLE set_rp (n UInt64) ENGINE = Set;
INSERT INTO set_rp VALUES (4242);
CREATE TABLE mt_table (n UInt64) ENGINE = MergeTree ORDER BY n;
INSERT INTO mt_table VALUES (4242), (31337);

DROP USER IF EXISTS $user;
CREATE USER $user IDENTIFIED WITH no_password;
-- Engine usage is a precondition of the two DDL cases below, not part of the grant progression
-- under test: where table_engines_require_grant is on, a MergeTree CREATE without this grant is
-- denied for the engine, which would satisfy the projection assertion without reaching the set.
GRANT TABLE ENGINE ON MergeTree TO $user;
"

# Without any grant, the set contents are readable neither directly nor through the right of IN.
$CLICKHOUSE_CLIENT --user "$user" -m -q "SELECT * FROM set_table; -- { serverError ACCESS_DENIED }"
$CLICKHOUSE_CLIENT --user "$user" -m -q "SELECT number FROM numbers(100000) WHERE number IN set_table; -- { serverError ACCESS_DENIED }"
# An ordinary table on the right of IN requires SELECT; the set table must behave the same.
$CLICKHOUSE_CLIENT --user "$user" -m -q "SELECT number FROM numbers(100000) WHERE number IN mt_table; -- { serverError ACCESS_DENIED }"

# INSERT is the grant one gets to add to a blocklist without being allowed to read it.
$CLICKHOUSE_CLIENT -m -q "GRANT INSERT ON $db.set_table TO $user; GRANT INSERT ON $db.mt_table TO $user"

$CLICKHOUSE_CLIENT --user "$user" -m -q "SELECT number FROM numbers(100000) WHERE number IN set_table; -- { serverError ACCESS_DENIED }"
# A query that declares itself secondary keeps enable_analyzer = 0, i.e. the old analysis path.
$CLICKHOUSE_CLIENT --user "$user" --query_kind secondary_query --enable_analyzer 0 -m -q "SELECT number FROM numbers(100000) WHERE number IN set_table; -- { serverError ACCESS_DENIED }"
# The same INSERT grant on an ordinary table does not open it either: SELECT is what is required.
$CLICKHOUSE_CLIENT --user "$user" -m -q "SELECT number FROM numbers(100000) WHERE number IN mt_table; -- { serverError ACCESS_DENIED }"

# A TTL WHERE is analyzed in the context of the user running the INSERT. A TTL expression is not
# built at DDL time, so the CREATE below succeeds and the denial lands on the INSERT.
$CLICKHOUSE_CLIENT -m -q "GRANT CREATE TABLE, INSERT ON $db.ttl_table TO $user"
$CLICKHOUSE_CLIENT --user "$user" -m -q "
CREATE TABLE ttl_table (x UInt64, d DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY x
    TTL d + INTERVAL 1 SECOND DELETE WHERE x IN $db.set_table;
"
$CLICKHOUSE_CLIENT --user "$user" -m -q "INSERT INTO ttl_table (x) SELECT 1; -- { serverError ACCESS_DENIED }"

# A projection WHERE, unlike a TTL, is analyzed by a full InterpreterSelectQuery at CREATE time,
# so that one needs the grant already at DDL, exactly as an ordinary table there already did.
$CLICKHOUSE_CLIENT -m -q "GRANT CREATE TABLE ON $db.proj_table TO $user"
$CLICKHOUSE_CLIENT --user "$user" -m -q "
CREATE TABLE proj_table (n UInt64, v UInt64, PROJECTION pr (SELECT n WHERE n IN $db.set_table ORDER BY n))
    ENGINE = MergeTree ORDER BY n; -- { serverError ACCESS_DENIED }
"

# The check covers every physical column of the set table, so a grant on part of them is not enough.
$CLICKHOUSE_CLIENT -m -q "GRANT SELECT(a) ON $db.set_pair TO $user"
$CLICKHOUSE_CLIENT --user "$user" -m -q "SELECT number FROM numbers(100000) WHERE (number, 1) IN set_pair ORDER BY number; -- { serverError ACCESS_DENIED }"
# ... and neither is a grant on the other column alone: the check covers every physical column.
$CLICKHOUSE_CLIENT -m -q "GRANT SELECT(b) ON $db.set_pair_b TO $user"
$CLICKHOUSE_CLIENT --user "$user" -m -q "SELECT number FROM numbers(100000) WHERE (number, 1) IN set_pair_b ORDER BY number; -- { serverError ACCESS_DENIED }"
$CLICKHOUSE_CLIENT -m -q "GRANT SELECT(a, b) ON $db.set_pair TO $user"
$CLICKHOUSE_CLIENT --user "$user" -m -q "SELECT number FROM numbers(100000) WHERE (number, 1) IN set_pair ORDER BY number"
$CLICKHOUSE_CLIENT -m -q "GRANT SELECT(a, b) ON $db.set_pair_b TO $user"
$CLICKHOUSE_CLIENT --user "$user" -m -q "SELECT number FROM numbers(100000) WHERE (number, 1) IN set_pair_b ORDER BY number"

# A MATERIALIZED column belongs to a set table's stored tuple, since the set is keyed on the sample
# block of the metadata, so its values are readable through IN and the grant has to cover it too.
# Only the old analysis path reaches this shape: the query tree derives the right-hand arity from the
# ordinary columns alone, so it rejects the pair before any set is built.
$CLICKHOUSE_CLIENT -m -q "GRANT SELECT(a) ON $db.set_mat TO $user"
$CLICKHOUSE_CLIENT --user "$user" --query_kind secondary_query --enable_analyzer 0 -m -q "SELECT number FROM numbers(100000) WHERE (4242, number) IN set_mat ORDER BY number; -- { serverError ACCESS_DENIED }"
$CLICKHOUSE_CLIENT -m -q "GRANT SELECT(a, m) ON $db.set_mat TO $user"
$CLICKHOUSE_CLIENT --user "$user" --query_kind secondary_query --enable_analyzer 0 -m -q "SELECT number FROM numbers(100000) WHERE (4242, number) IN set_mat ORDER BY number"

$CLICKHOUSE_CLIENT -m -q "GRANT SELECT ON $db.set_table TO $user"

$CLICKHOUSE_CLIENT --user "$user" -m -q "SELECT number FROM numbers(100000) WHERE number IN set_table ORDER BY number"
$CLICKHOUSE_CLIENT --user "$user" --query_kind secondary_query --enable_analyzer 0 -m -q "SELECT number FROM numbers(100000) WHERE number IN set_table ORDER BY number"
$CLICKHOUSE_CLIENT --user "$user" -m -q "INSERT INTO ttl_table (x) SELECT 1"

# The check is column-level, so a grant covering every column of the set table is enough.
$CLICKHOUSE_CLIENT -m -q "REVOKE SELECT ON $db.set_table FROM $user; GRANT SELECT(n) ON $db.set_table TO $user"
$CLICKHOUSE_CLIENT --user "$user" -m -q "SELECT number FROM numbers(100000) WHERE number IN set_table ORDER BY number"

# A row policy on the set table cannot filter a set that is already built, so a probe against it is
# refused for as long as the policy applies; an ordinary table there is filtered by the policy instead.
$CLICKHOUSE_CLIENT -m -q "GRANT SELECT ON $db.set_rp TO $user; CREATE ROW POLICY rp_set ON $db.set_rp USING n > 4242 TO $user"
$CLICKHOUSE_CLIENT --user "$user" -m -q "SELECT number FROM numbers(100000) WHERE number IN set_rp; -- { serverError ACCESS_DENIED }"
$CLICKHOUSE_CLIENT --user "$user" --query_kind secondary_query --enable_analyzer 0 -m -q "SELECT number FROM numbers(100000) WHERE number IN set_rp; -- { serverError ACCESS_DENIED }"
# A policy that hides nothing is not in the way of the probe.
$CLICKHOUSE_CLIENT -m -q "DROP ROW POLICY rp_set ON $db.set_rp; CREATE ROW POLICY rp_set ON $db.set_rp USING 1 TO $user"
$CLICKHOUSE_CLIENT --user "$user" -m -q "SELECT number FROM numbers(100000) WHERE number IN set_rp ORDER BY number"
$CLICKHOUSE_CLIENT -m -q "DROP ROW POLICY rp_set ON $db.set_rp"

$CLICKHOUSE_CLIENT -m -q "DROP USER $user"
