#!/usr/bin/env bash
# Tags: race, no-fasttest
# Concurrently CREATE/DROP a row policy while reading the protected table. The filter
# rebuild now runs off the cache lock, so this stresses the publish-vs-read path for
# TSan/ASan. A stable RESTRICTIVE policy (c < 500) keeps the visible count at 500
# regardless of the toggled second RESTRICTIVE policy (c < 1000), since both AND together.
# Policies are scoped to a dedicated user (not TO ALL) so parallel tests are unaffected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

P_STABLE="stable_${CLICKHOUSE_DATABASE}"
P_TOGGLE="toggle_${CLICKHOUSE_DATABASE}"
USER="user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
DROP ROW POLICY IF EXISTS ${P_TOGGLE} ON tbl;
DROP ROW POLICY IF EXISTS ${P_STABLE} ON tbl;
DROP USER IF EXISTS ${USER};
CREATE USER ${USER};
CREATE TABLE tbl (c UInt64) ENGINE = MergeTree ORDER BY c;
INSERT INTO tbl SELECT number FROM numbers(1000);
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.tbl TO ${USER};
CREATE ROW POLICY ${P_STABLE} ON tbl USING c < 500 AS RESTRICTIVE TO ${USER};
"

TIMEOUT=10

function toggle()
{
    local end=$((SECONDS + TIMEOUT))
    while [ $SECONDS -lt $end ]; do
        $CLICKHOUSE_CLIENT -q "CREATE ROW POLICY ${P_TOGGLE} ON tbl USING c < 1000 AS RESTRICTIVE TO ${USER}"
        $CLICKHOUSE_CLIENT -q "DROP ROW POLICY ${P_TOGGLE} ON tbl"
    done
}

function read_count()
{
    local end=$((SECONDS + TIMEOUT))
    while [ $SECONDS -lt $end ]; do
        res=$($CLICKHOUSE_CLIENT --user "${USER}" -q "SELECT count() FROM tbl")
        [ "$res" = "500" ] || echo "FAIL: count = $res"
    done
}

toggle &
read_count &
read_count &
wait

$CLICKHOUSE_CLIENT -q "
DROP ROW POLICY IF EXISTS ${P_TOGGLE} ON tbl;
DROP ROW POLICY ${P_STABLE} ON tbl;
DROP TABLE tbl;
DROP USER ${USER};
"

echo OK
