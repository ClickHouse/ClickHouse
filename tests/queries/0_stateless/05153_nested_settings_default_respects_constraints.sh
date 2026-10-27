#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A nested `SETTINGS name = DEFAULT` (in a subquery, a CTE or a view's inner query) is applied to the
# per-node context by the query tree builder. A reset is an assignment of the declared default, so it
# has to pass the reader's settings constraints exactly like an explicit assignment in the same clause -
# otherwise `SETTINGS max_result_rows = DEFAULT` escapes a `MIN` / `MAX` bound, and
# `SETTINGS max_execution_time = DEFAULT` escapes a `CONST` constraint, which the written-out value
# cannot. A top-level clause throws on a violation; a nested one is clamped, like every other setting
# crossing an execution context.

USER="user_05153_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
DROP USER IF EXISTS ${USER};
CREATE USER ${USER} IDENTIFIED WITH no_password
    SETTINGS max_result_rows = 1000 MIN 500 MAX 2000, max_execution_time = 42 CONST;
GRANT SELECT ON *.* TO ${USER};
"

CLIENT_AS_USER="${CLICKHOUSE_CLIENT} --user ${USER}"

echo '-- the constrained values as seen by the user'
$CLIENT_AS_USER --query "SELECT getSetting('max_result_rows'), getSetting('max_execution_time')"

echo '-- a top-level reset still throws'
$CLIENT_AS_USER --query "SELECT getSetting('max_result_rows') SETTINGS max_result_rows = DEFAULT" 2>&1 | grep -q 'SETTING_CONSTRAINT_VIOLATION' && echo 'max_result_rows: constraint violation'
$CLIENT_AS_USER --query "SELECT getSetting('max_execution_time') SETTINGS max_execution_time = DEFAULT" 2>&1 | grep -q 'SETTING_CONSTRAINT_VIOLATION' && echo 'max_execution_time: constraint violation'

echo '-- a nested reset past a MIN bound is clamped to the bound, not set to the default'
$CLIENT_AS_USER --query "SELECT * FROM (SELECT getSetting('max_result_rows') SETTINGS max_result_rows = DEFAULT)"

echo '-- a nested reset of a CONST setting is dropped'
$CLIENT_AS_USER --query "SELECT * FROM (SELECT getSetting('max_execution_time') SETTINGS max_execution_time = DEFAULT)"

echo '-- the same holds for a CTE'
$CLIENT_AS_USER --query "WITH c AS (SELECT getSetting('max_result_rows') AS v SETTINGS max_result_rows = DEFAULT) SELECT v FROM c"

echo '-- a duplicated setting in a nested clause is last-wins, and the last value is clamped as well'
$CLIENT_AS_USER --query "SELECT * FROM (SELECT getSetting('max_result_rows') SETTINGS max_result_rows = 700, max_result_rows = 1000)"
$CLIENT_AS_USER --query "SELECT * FROM (SELECT getSetting('max_result_rows') SETTINGS max_result_rows = 700, max_result_rows = 9000)"

echo '-- an unconstrained nested reset is unaffected'
$CLIENT_AS_USER --query "SELECT * FROM (SELECT getSetting('max_rows_to_read') SETTINGS max_rows_to_read = DEFAULT)"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${USER};"
