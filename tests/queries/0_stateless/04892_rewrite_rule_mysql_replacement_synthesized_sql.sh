#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: rewrite rules are global server state
# Tag no-fasttest: requires mysql client

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The MySQL wire protocol emulates a few MySQL commands by substituting ClickHouse SQL of its own
# (`SHOW WARNINGS`, `SHOW VARIABLES`, ...). The client never submitted that SQL, so an active
# `query_rules` matching the substituted text must not reject (or rewrite) the MySQL command.

${CLICKHOUSE_CLIENT} -q "
CREATE RULE rule_04892_show_variables AS (SELECT '') REJECT WITH 'blocked_04892_variables';
CREATE RULE rule_04892_show_warnings AS (SELECT '' AS Level, 0::UInt32 AS Code, '' AS Message WHERE false) REJECT WITH 'blocked_04892_warnings';
"

# Sanity check: the rules do fire when the user submits the same SQL directly.
${CLICKHOUSE_CLIENT} -q "SET query_rules = 'rule_04892_show_variables'; SELECT '';" 2>&1 \
    | grep -o -m1 'blocked_04892_variables'
${CLICKHOUSE_CLIENT} -q "SET query_rules = 'rule_04892_show_warnings'; SELECT '' AS Level, 0::UInt32 AS Code, '' AS Message WHERE false;" 2>&1 \
    | grep -o -m1 'blocked_04892_warnings'

# The emulated MySQL commands must succeed even though the session activates the rules matching
# their substituted implementation SQL.
${MYSQL_CLIENT} --execute "
SET query_rules = 'rule_04892_show_variables, rule_04892_show_warnings';
SHOW VARIABLES;
SHOW WARNINGS;
SELECT 'mysql_ok_04892';
" 2>&1

${CLICKHOUSE_CLIENT} -q "
DROP RULE rule_04892_show_variables;
DROP RULE rule_04892_show_warnings;
"
