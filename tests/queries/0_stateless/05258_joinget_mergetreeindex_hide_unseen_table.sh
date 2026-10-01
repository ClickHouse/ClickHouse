#!/usr/bin/env bash

# `joinGet` and `mergeTreeIndex` resolve the table named in their arguments before the column-level `SELECT` check.
# A user who cannot see the table must get `ACCESS_DENIED` there, not an error or a structure that reveals whether
# the table exists, which engine it has, or which columns its key consists of.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05258_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user;
CREATE TABLE join_t (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO join_t VALUES (1, 'a');
CREATE TABLE mt (k UInt64, secret String) ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO mt VALUES (1, 'x');
CREATE USER $user;
"

function run_as_user()
{
    local output
    if output=$($CLICKHOUSE_CLIENT --user "$user" -q "$1" 2>&1); then
        echo "OK: $output"
    elif grep -q "ACCESS_DENIED" <<< "$output"; then
        echo "ACCESS_DENIED"
    else
        echo "$output" | grep -o 'Code: [0-9]*' | head -n1
    fi
}

echo "-- joinGet, no grants: an existing Join table, a non-Join table and a missing table look the same"
run_as_user "SELECT joinGet('$CLICKHOUSE_DATABASE.join_t', 'v', 1::UInt64)"
run_as_user "SELECT joinGet('$CLICKHOUSE_DATABASE.mt', 'secret', 1::UInt64)"
run_as_user "SELECT joinGet('$CLICKHOUSE_DATABASE.missing', 'v', 1::UInt64)"

echo "-- mergeTreeIndex, no grants: DESCRIBE and SELECT reveal nothing"
run_as_user "DESCRIBE mergeTreeIndex('$CLICKHOUSE_DATABASE', 'mt')"
run_as_user "DESCRIBE mergeTreeIndex('$CLICKHOUSE_DATABASE', 'missing')"
run_as_user "DESCRIBE mergeTreeIndex('$CLICKHOUSE_DATABASE', 'join_t')"
run_as_user "SELECT count() FROM mergeTreeIndex('$CLICKHOUSE_DATABASE', 'mt')"

$CLICKHOUSE_CLIENT -q "GRANT SELECT(k) ON $CLICKHOUSE_DATABASE.join_t TO $user; GRANT SELECT(k) ON $CLICKHOUSE_DATABASE.mt TO $user;"

echo "-- a grant on one column makes the table visible, and the column-level check applies as before"
run_as_user "SELECT joinGet('$CLICKHOUSE_DATABASE.join_t', 'v', 1::UInt64)"
run_as_user "SELECT k FROM mergeTreeIndex('$CLICKHOUSE_DATABASE', 'mt')"
run_as_user "SELECT \`secret.mark\` FROM mergeTreeIndex('$CLICKHOUSE_DATABASE', 'mt', with_marks = true)"

$CLICKHOUSE_CLIENT -q "GRANT SELECT(v) ON $CLICKHOUSE_DATABASE.join_t TO $user;"
run_as_user "SELECT joinGet('$CLICKHOUSE_DATABASE.join_t', 'v', 1::UInt64)"

$CLICKHOUSE_CLIENT -q "DROP USER $user"
