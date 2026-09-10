#!/usr/bin/env bash
# Regression test: SHOW CREATE TABLE and DESCRIBE TABLE must not disclose the
# names/types/codecs of restricted columns to a user holding only a partial
# (column-level) grant. A column-level grant implies SHOW COLUMNS only for the
# granted columns, never the whole table, so both statements must be denied
# rather than dumping the full DDL. Fixed by ef9450e69d6 ("Fix implicit
# privileges (worked as wildcard before)").

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user04365_$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS tbl;
CREATE TABLE tbl (x UInt32, y UInt32, z UInt32 CODEC(ZSTD)) ENGINE = MergeTree ORDER BY x;
DROP USER IF EXISTS $user;
CREATE USER $user;
"

run_user() {
    # Print only the line of interest; collapse the access-denied error to a
    # stable token so the .reference does not depend on the exact wording.
    $CLICKHOUSE_CLIENT --user "$user" -q "$1" 2>&1 \
        | grep -oE "ACCESS_DENIED|^CREATE TABLE|^x$|^y$|^z$" | head -1
}

echo "--- partial column SELECT grant: must NOT leak column z ---"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(x, y) ON ${CLICKHOUSE_DATABASE}.tbl TO $user"
echo -n "SHOW CREATE: "; run_user "SHOW CREATE TABLE tbl"
echo -n "DESCRIBE:    "; run_user "DESCRIBE TABLE tbl"
echo -n "z via DESCRIBE name col present? "
$CLICKHOUSE_CLIENT --user "$user" -q "DESCRIBE TABLE tbl" 2>&1 | grep -qw z && echo "LEAK" || echo "no"

echo "--- partial SHOW COLUMNS grant: must NOT leak column z ---"
$CLICKHOUSE_CLIENT -q "GRANT SHOW COLUMNS(x, y) ON ${CLICKHOUSE_DATABASE}.tbl TO $user"
echo -n "SHOW CREATE: "; run_user "SHOW CREATE TABLE tbl"
echo -n "DESCRIBE:    "; run_user "DESCRIBE TABLE tbl"

echo "--- all columns granted individually still does not imply table-level SHOW COLUMNS ---"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(x, y, z) ON ${CLICKHOUSE_DATABASE}.tbl TO $user"
echo -n "SHOW CREATE: "; run_user "SHOW CREATE TABLE tbl"
echo -n "DESCRIBE:    "; run_user "DESCRIBE TABLE tbl"

echo "--- full table SELECT grant: SHOW CREATE / DESCRIBE legitimately work ---"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.tbl TO $user"
echo -n "SHOW CREATE: "; run_user "SHOW CREATE TABLE tbl"
echo "DESCRIBE columns:"; $CLICKHOUSE_CLIENT --user "$user" -q "DESCRIBE TABLE tbl" | awk '{print $1}'

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE tbl;
DROP USER $user;
"
