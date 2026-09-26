#!/usr/bin/env bash

# A subcolumn inherits the `SELECT` grant of its column, and a GRANT or REVOKE on the exact subcolumn name
# takes precedence over it, the same way a column grant takes precedence over its table.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="u_05257_${CLICKHOUSE_DATABASE}"
role="r_05257_${CLICKHOUSE_DATABASE}"
t="${CLICKHOUSE_DATABASE}.t"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user;
DROP ROLE IF EXISTS $role;
CREATE TABLE t (p Tuple(s String, o String), j JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t VALUES (('s', 'o'), '{\"a\": \"a\", \"b\": \"b\"}');
CREATE USER $user;
CREATE ROLE $role;
"

# Prints `+` or `-` for each of `p`, `p.s`, `p.o`, `j.a`, `j.b` read by the user.
check()
{
    local line="$1:" out
    for column in p p.s p.o j.a j.b; do
        if out=$($CLICKHOUSE_CLIENT --user "$user" -q "SELECT $column FROM t FORMAT Null" 2>&1); then
            line="$line $column+"
        elif echo "$out" | grep -q 'ACCESS_DENIED'; then
            line="$line $column-"
        else
            line="$line $column:$out"
        fi
    done
    echo "$line"
}

reset()
{
    $CLICKHOUSE_CLIENT -q "REVOKE ALL ON *.* FROM $user; REVOKE ALL ON *.* FROM $role; REVOKE $role FROM $user"
}

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(p) ON $t TO $user"
check "GRANT(p)"

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(\`p.s\`) ON $t TO $user"
check "GRANT(p.s)"

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT ON $t TO $user; REVOKE SELECT(p) ON $t FROM $user"
check "GRANT table, REVOKE(p)"

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT ON $t TO $user; REVOKE SELECT(\`p.s\`) ON $t FROM $user"
check "GRANT table, REVOKE(p.s)"

# The subcolumn inherits nothing that could be revoked, so this REVOKE has no effect.
reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(p) ON $t TO $user; REVOKE SELECT(\`p.s\`) ON $t FROM $user"
check "GRANT(p), REVOKE(p.s)"

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(\`p.s\`) ON $t TO $user; REVOKE SELECT(p) ON $t FROM $user"
check "GRANT(p.s), REVOKE(p)"

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(\`j.a\`) ON $t TO $user"
check "GRANT(j.a)"

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT ON $t TO $user; REVOKE SELECT(\`j.a\`) ON $t FROM $user"
check "GRANT table, REVOKE(j.a)"

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(p) ON $t TO $role; GRANT $role TO $user"
check "role: GRANT(p)"

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT ON $t TO $role; REVOKE SELECT(p) ON $t FROM $role; GRANT $role TO $user"
check "role: GRANT table, REVOKE(p)"

reset; $CLICKHOUSE_CLIENT -q "GRANT SELECT(\`p.s\`) ON $t TO $role; GRANT $role TO $user"
check "role: GRANT(p.s)"

$CLICKHOUSE_CLIENT -q "
DROP USER $user;
DROP ROLE $role;
DROP TABLE t;
"
