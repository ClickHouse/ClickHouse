#!/usr/bin/env bash

# Regression for per-child column resolution in StorageMerge access checks. A Merge table over
# heterogeneous children must re-resolve each requested dotted identifier against EACH child's own
# (locked) schema, because the same name can mean different things:
#   - in `src_tuple` the column `a` is a `Tuple(b UInt8)`, so `a.b` is a subcolumn and maps to `a`
#     (i.e. `GRANT SELECT(a)` authorizes it, `GRANT SELECT(`a.b`)` does not);
#   - in `src_real` there is a real top-level column literally named `a.b`, so `a.b` maps to itself
#     (i.e. `GRANT SELECT(`a.b`)` authorizes it, `GRANT SELECT(a)` does not).
# The access mapping in StorageMerge::createSources must stay aligned with the actual child read path:
# each allow case therefore checks that the correct grant both passes the access check AND returns the
# expected value; each swapped grant must be denied. Children the user cannot SHOW are skipped, which
# lets us exercise one child at a time by granting only on that child.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="test_user_04511_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS src_tuple;
DROP TABLE IF EXISTS src_real;
DROP TABLE IF EXISTS mrg;
DROP USER IF EXISTS $user;

CREATE TABLE src_tuple (a Tuple(b UInt8)) ENGINE = Memory;
INSERT INTO src_tuple VALUES ((11));

CREATE TABLE src_real (\`a.b\` UInt8) ENGINE = Memory;
INSERT INTO src_real VALUES (22);

CREATE TABLE mrg (\`a.b\` UInt8) ENGINE = Merge(currentDatabase(), '^src_');

CREATE USER $user;
-- Access to the Merge table itself; reads are still authorized per underlying child below.
GRANT SELECT ON $CLICKHOUSE_DATABASE.mrg TO $user;
"

query="SELECT \`a.b\` FROM mrg ORDER BY \`a.b\`"

# Run a query as the restricted user: print its rows on success, or ACCESS_DENIED (or the raw error).
run()
{
    local label="$1" out
    echo "=== $label ==="
    out=$($CLICKHOUSE_CLIENT --user "$user" -q "$query" 2>&1)
    if [ $? -eq 0 ]; then
        echo "$out"
    elif echo "$out" | grep -q 'ACCESS_DENIED'; then
        echo "ACCESS_DENIED"
    else
        echo "$out"
    fi
}

# Tuple child only (real child invisible): `a.b` maps to `a`, so SELECT(a) authorizes it.
$CLICKHOUSE_CLIENT -q "GRANT SELECT(a) ON $CLICKHOUSE_DATABASE.src_tuple TO $user;"
run "tuple child: SELECT(a) authorizes a.b"
$CLICKHOUSE_CLIENT -q "REVOKE SELECT(a) ON $CLICKHOUSE_DATABASE.src_tuple FROM $user;"

# Tuple child only: SELECT(`a.b`) is the wrong grant here (a.b maps to a), so it must be denied.
$CLICKHOUSE_CLIENT -q "GRANT SELECT(\`a.b\`) ON $CLICKHOUSE_DATABASE.src_tuple TO $user;"
run "tuple child: SELECT(\`a.b\`) does not authorize a.b"
$CLICKHOUSE_CLIENT -q "REVOKE SELECT(\`a.b\`) ON $CLICKHOUSE_DATABASE.src_tuple FROM $user;"

# Real child only (tuple child invisible): `a.b` maps to itself, so SELECT(`a.b`) authorizes it.
$CLICKHOUSE_CLIENT -q "GRANT SELECT(\`a.b\`) ON $CLICKHOUSE_DATABASE.src_real TO $user;"
run "real child: SELECT(\`a.b\`) authorizes a.b"
$CLICKHOUSE_CLIENT -q "REVOKE SELECT(\`a.b\`) ON $CLICKHOUSE_DATABASE.src_real FROM $user;"

# Real child only: SELECT(a) is the wrong grant here (a.b is a real column), so it must be denied.
$CLICKHOUSE_CLIENT -q "GRANT SELECT(a) ON $CLICKHOUSE_DATABASE.src_real TO $user;"
run "real child: SELECT(a) does not authorize a.b"
$CLICKHOUSE_CLIENT -q "REVOKE SELECT(a) ON $CLICKHOUSE_DATABASE.src_real FROM $user;"

# Both children with their correct (different) grants: the read returns a value from each child.
$CLICKHOUSE_CLIENT -q "GRANT SELECT(a) ON $CLICKHOUSE_DATABASE.src_tuple TO $user;"
$CLICKHOUSE_CLIENT -q "GRANT SELECT(\`a.b\`) ON $CLICKHOUSE_DATABASE.src_real TO $user;"
run "both children: correct per-child grants"
$CLICKHOUSE_CLIENT -q "REVOKE SELECT(a) ON $CLICKHOUSE_DATABASE.src_tuple FROM $user;"
$CLICKHOUSE_CLIENT -q "REVOKE SELECT(\`a.b\`) ON $CLICKHOUSE_DATABASE.src_real FROM $user;"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS src_tuple;
    DROP TABLE IF EXISTS src_real;
    DROP TABLE IF EXISTS mrg;
    DROP USER IF EXISTS $user;
"
