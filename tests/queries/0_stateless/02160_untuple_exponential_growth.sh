#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Should finish in reasonable time (milliseconds).
# In previous versions this query led to exponential complexity of query analysis.

${CLICKHOUSE_LOCAL} --query "SELECT untuple(tuple(untuple((1, untuple((untuple(tuple(untuple(tuple(untuple((untuple((1, 1, 1, 1)), 1, 1, 1)))))), 1, 1))))))" 2>&1 | grep -cF 'TOO_BIG_AST'
${CLICKHOUSE_LOCAL} --query "SELECT untuple(tuple(untuple(tuple(untuple(tuple(untuple(tuple(untuple(tuple(untuple(tuple(untuple(tuple(untuple(tuple(untuple(tuple(untuple(tuple(untuple(tuple(untuple(tuple(untuple((1, 1, 1, 1, 1))))))))))))))))))))))))))" 2>&1 | grep -cF 'TOO_BIG_AST'
