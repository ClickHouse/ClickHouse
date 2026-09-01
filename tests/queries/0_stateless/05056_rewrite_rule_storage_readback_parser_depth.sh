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

# Nested deeper than the default `max_parser_depth` of 1000, but accepted under the raised limits.
DEEP_EXPRESSION=$(python3 -c "print('('*400 + '1' + ')'*400)")

${CLICKHOUSE_LOCAL} --path "${RULE_DIR}" -q "
SET max_parser_depth = 6000, max_parser_backtracks = 10000000, max_ast_depth = 6000, max_ast_elements = 100000;
CREATE RULE rule_05056_deep AS (SELECT ${DEEP_EXPRESSION}) REWRITE TO (SELECT 'deep');
SELECT name FROM system.query_rules WHERE name = 'rule_05056_deep';
"

# A fresh process with default settings loads the rule from storage.
${CLICKHOUSE_LOCAL} --path "${RULE_DIR}" -q "SELECT name FROM system.query_rules WHERE name = 'rule_05056_deep'"

# The loaded rule still works: it is applied to the same deeply nested query.
${CLICKHOUSE_LOCAL} --path "${RULE_DIR}" -q "
SET max_parser_depth = 6000, max_parser_backtracks = 10000000, max_ast_depth = 6000, max_ast_elements = 100000, query_rules = 'rule_05056_deep';
SELECT ${DEEP_EXPRESSION};
"

rm -rf "${RULE_DIR}"
