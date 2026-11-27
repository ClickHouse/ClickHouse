#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: uses `clickhouse-local` with its own data directory

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A rewrite rule is persisted as the server's own canonical `CREATE RULE` text and re-parsed when
# the rule storage is loaded (server start, `system.query_rules`, `ALTER RULE`, background reload).
# That re-parse must not apply the reader's `max_parser_depth` / `max_parser_backtracks`: a rule
# created in a session with raised limits would otherwise become unreadable later under the default
# ones, and the server's own valid output would be rejected. Two `clickhouse-local` runs over the
# same data directory reproduce the create-then-load sequence: the second one loads the rule with
# default settings.

RULE_DIR="${CLICKHOUSE_TMP}/rewrite_rule_storage_05056"
rm -rf "${RULE_DIR}"
mkdir -p "${RULE_DIR}"

# Each parenthesis level costs several parser recursion levels, so 100 levels need a
# `max_parser_depth` of about 600: well above the reduced limit the reader below runs with, and
# shallow enough that the parser stays within the TSan stack budget (`checkStackSize` allows only
# 5% of the stack under TSan, which 400 levels exhausted).
DEEP_EXPRESSION=$(python3 -c "print('('*100 + '1' + ')'*100)")

${CLICKHOUSE_LOCAL} --path "${RULE_DIR}" -q "
SET max_parser_depth = 2000, max_parser_backtracks = 10000000, max_ast_depth = 2000, max_ast_elements = 100000;
CREATE RULE rule_05056_deep AS (SELECT ${DEEP_EXPRESSION}) REWRITE TO (SELECT 'deep');
SELECT name FROM system.query_rules WHERE name = 'rule_05056_deep';
"

# A fresh process with default settings loads the rule from storage.
${CLICKHOUSE_LOCAL} --path "${RULE_DIR}" -q "SELECT name FROM system.query_rules WHERE name = 'rule_05056_deep'"

# So does a process whose `max_parser_depth` is far below what the stored rule needs: the reader's
# limits must not apply to the server's own persisted output.
${CLICKHOUSE_LOCAL} --path "${RULE_DIR}" --max_parser_depth 200 -q "SELECT name FROM system.query_rules WHERE name = 'rule_05056_deep'"

# The loaded rule still works: it is applied to the same deeply nested query.
${CLICKHOUSE_LOCAL} --path "${RULE_DIR}" -q "
SET max_parser_depth = 2000, max_parser_backtracks = 10000000, max_ast_depth = 2000, max_ast_elements = 100000, query_rules = 'rule_05056_deep';
SELECT ${DEEP_EXPRESSION};
"

rm -rf "${RULE_DIR}"
