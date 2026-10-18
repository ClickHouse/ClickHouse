#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A rewrite rule must apply only to the initial user query, never to an internal helper query.
# Several user-facing statements are implemented by copying the user's context and re-entering
# `executeQuery(..., QueryFlags{ .internal = true })` with a different SQL body: for example
# `SHOW PRIVILEGES` runs `SELECT * FROM system.privileges`. The helper query inherits the
# session's `query_rules`, so a rule matching the implementation SQL must NOT affect the
# `SHOW PRIVILEGES` statement, while it must still affect a direct user query of the same SQL.

# The rule name is global, so it is made unique per test database. `DROP RULE` has no
# `IF EXISTS` form, so a leftover from a previous failed run is dropped defensively.
RULE="rule_internal_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}" 2>/dev/null
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE} AS (SELECT * FROM system.privileges) REJECT WITH 'rejected by ${RULE}'"

# `SHOW PRIVILEGES` is implemented internally as `SELECT * FROM system.privileges`. With the
# rule active the statement must still succeed: the rule matches the internal helper query, not
# the `SHOW PRIVILEGES` the user submitted, so it must not be rejected.
echo "SHOW PRIVILEGES with rule active:"
$CLICKHOUSE_CLIENT --query_rules "${RULE}" --query "SHOW PRIVILEGES FORMAT Null" 2>&1 | grep -o -m1 "REWRITE_RULE_REJECTION" || echo "not rejected"

# The same query submitted directly by the user is the initial query, so the rule still applies
# and rejects it. This proves the guard skips only internal helper queries, not user queries.
echo "direct user query with rule active:"
$CLICKHOUSE_CLIENT --query_rules "${RULE}" --query "SELECT * FROM system.privileges" 2>&1 | grep -o -m1 "REWRITE_RULE_REJECTION" || echo "not rejected"

$CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}"
