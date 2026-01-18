#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test repeated alias for literal as 2nd arg to IN operator
echo "SELECT ([1] AS foo), [1] IN ([1] AS foo);" | "$CLICKHOUSE_FORMAT"

# Test repeated alias for statement which is not a literal
echo "SELECT ([isNaN(1)] AS foo), [1] IN ([isNaN(1)] AS foo);" | "$CLICKHOUSE_FORMAT"

# Test repeated alias for negated col
echo "SELECT (-((-\`c1\`) AS \`a2\`)), NOT (-((-\`c1\`) AS \`a2\`)) from tab;" | "$CLICKHOUSE_FORMAT"

# Test repeated alias in subquery after IN
echo "SELECT ((SELECT [1,2,3]) AS a1), [5] IN ((SELECT [1,2,3]) AS a1);" | "$CLICKHOUSE_FORMAT"

# Test repeated alias in subquery after NOT
echo "SELECT ((SELECT 1) AS a1), NOT ((SELECT 1) AS a1);" | "$CLICKHOUSE_FORMAT"

# Test repeated alias for tuple after IN
echo "SELECT tuple(1, 'a') as a1, tuple(1, 'a') IN (tuple(1, 'a') as a1);" | "$CLICKHOUSE_FORMAT"
