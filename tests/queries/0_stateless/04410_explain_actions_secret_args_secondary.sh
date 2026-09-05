#!/usr/bin/env bash
# Tags: no-fasttest, no-old-analyzer
# Tag no-fasttest: the encryption functions are not available in the fast test build
# Tag no-old-analyzer: the old analyzer builds the ActionsDAG without query-tree masking, so it still leaks the key

# On a secondary (shard) query the planner skips AST-level optimizations, so a secret argument
# folded into a constant (e.g. concat('SECRET_', 'KEY')) used to be named by its source expression,
# leaking the pieces in the ActionsDAG dump and in distributed headers/logs. It must be hidden, and
# named identically to the initiator so distributed headers still match.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

QUERY="EXPLAIN actions = 1 SELECT encrypt('aes-128-ecb', toString(number), concat('SECRET_', 'KEY')) FROM numbers(1) SETTINGS explain_query_plan_default = 'legacy'"

echo "-- secondary query, secrets hidden by default"
${CLICKHOUSE_CLIENT} --query_kind secondary_query --query "${QUERY}"
