#!/usr/bin/env bash
# Tags: no-fasttest

# Verify that predicate pushdown on system.users by the name column works correctly.
# The fast path (direct AccessControl lookups) is used for name = 'literal' and name IN (...)
# predicates, the full scan is used otherwise.
#
# `ReadFromSystemOneBlock` emits exactly the rows that `fillData` produces as a single chunk,
# so the `read_rows` recorded in `system.query_log` is a reliable witness of which path ran:
# the fast path materializes only the matching users, while the full-scan fallback materializes
# every user on the server. (`max_rows_to_read` is intentionally not used as the witness: it is
# not enforced for this single-chunk source.)

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Unique, collision-free names so the test is safe to run in parallel with others.
P="u_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${P}_alice, ${P}_bob, ${P}_carol"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${P}_alice, ${P}_bob, ${P}_carol"

# Run a query under a unique query_id (discarding its output), then print how many rows its
# source produced. A small, deterministic number proves the fast path ran; the fallback would
# read every user on the server instead.
read_rows() {
    local qid="$1"
    ${CLICKHOUSE_CLIENT} --query_id "${qid}" -q "$2" >/dev/null
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT read_rows FROM system.query_log
        WHERE query_id = '${qid}' AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC LIMIT 1"
}

echo "-- equality fast path: returns the user and reads only the matched row"
${CLICKHOUSE_CLIENT} -q "SELECT replaceOne(name, '${P}_', '') FROM system.users WHERE name = '${P}_alice'"
read_rows "${P}_eq" "SELECT name FROM system.users WHERE name = '${P}_alice' FORMAT Null"

echo "-- equality fast path: a non-existent user returns nothing and reads no rows"
${CLICKHOUSE_CLIENT} -q "SELECT replaceOne(name, '${P}_', '') FROM system.users WHERE name = '${P}_nonexistent'"
read_rows "${P}_eq_none" "SELECT name FROM system.users WHERE name = '${P}_nonexistent' FORMAT Null"

echo "-- IN fast path: returns both users and reads only the matched rows"
${CLICKHOUSE_CLIENT} -q "SELECT replaceOne(name, '${P}_', '') FROM system.users WHERE name IN ('${P}_alice', '${P}_bob') ORDER BY name"
read_rows "${P}_in" "SELECT name FROM system.users WHERE name IN ('${P}_alice', '${P}_bob') FORMAT Null"

echo "-- AND fast path: contradictory equalities intersect to the empty set"
${CLICKHOUSE_CLIENT} -q "SELECT replaceOne(name, '${P}_', '') FROM system.users WHERE name = '${P}_alice' AND name = '${P}_bob'"
read_rows "${P}_and_contra" "SELECT name FROM system.users WHERE name = '${P}_alice' AND name = '${P}_bob' FORMAT Null"

echo "-- AND fast path: equality intersected with IN narrows the candidates"
${CLICKHOUSE_CLIENT} -q "SELECT replaceOne(name, '${P}_', '') FROM system.users WHERE name = '${P}_alice' AND name IN ('${P}_alice', '${P}_bob')"
read_rows "${P}_and_in" "SELECT name FROM system.users WHERE name = '${P}_alice' AND name IN ('${P}_alice', '${P}_bob') FORMAT Null"

echo "-- AND fast path: equality combined with an unrelated condition still narrows by name"
${CLICKHOUSE_CLIENT} -q "SELECT replaceOne(name, '${P}_', '') FROM system.users WHERE name = '${P}_alice' AND default_database = ''"
read_rows "${P}_and_other" "SELECT name FROM system.users WHERE name = '${P}_alice' AND default_database = '' FORMAT Null"

echo "-- fallback: a constant alias named 'name' must not be mistaken for the column"
# Here 'name' in WHERE refers to the alias (a constant), so the predicate is constant-true and
# every user must be returned, just like the full scan.
${CLICKHOUSE_CLIENT} -q "SELECT count() > 1 FROM (SELECT '${P}_alice' AS name FROM system.users WHERE name = '${P}_alice')"

echo "-- fallback: LIKE predicate still returns the right users"
${CLICKHOUSE_CLIENT} -q "SELECT replaceOne(name, '${P}_', '') FROM system.users WHERE name LIKE '${P}_%' ORDER BY name"

echo "-- fallback: a large IN set (above the fast-path limit) falls back to the full scan but stays correct"
LARGE_IN=$(seq 1 1500 | sed "s/.*/'${P}_filler_&'/" | paste -sd, -)
${CLICKHOUSE_CLIENT} -q "SELECT replaceOne(name, '${P}_', '') FROM system.users WHERE name IN (${LARGE_IN}, '${P}_alice')"

echo "-- fallback: counting all users still works"
${CLICKHOUSE_CLIENT} -q "SELECT count() > 0 FROM system.users"

${CLICKHOUSE_CLIENT} -q "DROP USER ${P}_alice, ${P}_bob, ${P}_carol"
