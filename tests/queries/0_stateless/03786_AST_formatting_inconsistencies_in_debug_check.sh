#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ch_format() {
    $CLICKHOUSE_FORMAT --oneline
}

format_query() {
  local query="$1"
  echo "Original:          $query"
  local formatted="$(echo "$query" | ch_format)"
  echo "format(original):  $formatted"
  echo 'format(formatted):' "$(echo "$formatted" | ch_format)"
  echo
}

# Test repeated alias for literal as 2nd arg to IN operator
format_query "SELECT ([1] AS foo), [1] IN ([1] AS foo)"

# Test repeated alias for statement which is not a literal
format_query "SELECT ([isNaN(1)] AS foo), [1] IN ([isNaN(1)] AS foo)"

# Test repeated alias for negated col
format_query "SELECT (-((-\`c1\`) AS \`a2\`)), NOT (-((-\`c1\`) AS \`a2\`)) from tab"

# Test repeated alias in subquery after IN
format_query "SELECT ((SELECT [1,2,3]) AS a1), [5] IN ((SELECT [1,2,3]) AS a1)"

# Test repeated alias in subquery after NOT
format_query "SELECT ((SELECT 1) AS a1), NOT ((SELECT 1) AS a1)"

# Test repeated alias for tuple after IN
format_query "SELECT tuple(1, 'a') as a1, tuple(1, 'a') IN (tuple(1, 'a') as a1)"

# Test alias in ON clause of JOIN
format_query "SELECT * FROM t1 JOIN t2 ON ((t1.x = t2.x) AND (t1.x IS NULL) AS e2)"

# Various pathological queries, if the query is valid the formatter should mostly leave it alone
format_query "SELECT (1,1) c0, (1,(1)) c0"
format_query "SELECT (1, (1 AS c0, 1 AS c0) IS NULL AS c0), (1, (1, 1 AS c0) IS NULL AS c0) IS NULL AS c0"
format_query "SELECT NOT ((1, 1, 1))"
format_query "select (((1), (2)))"
format_query "select * from tuple('42', '3141592')"
format_query "SELECT 1 FROM VALUES (1, (NOT 1 IS NULL)) tx"

# Test that INSERT INTO with EXCEPT does not crash debug build
format_query "INSERT INTO tab2 SELECT * FROM tab EXCEPT SELECT * FROM tab;"

# Test weird SELECT/EXCEPT/SELECT statement
format_query "SELECT 1,2,3 EXCEPT SELECT 1,2,3;"

# Allow alias in MODIFY ORDER BY
format_query "ALTER TABLE \`t55\` (MODIFY ORDER BY ((\`c0\` AS \`a0\`)));"

# Complex tuple expression with index
format_query "select (tab.*).2 from tab;"
format_query "with (((1,1),1),1) as t1 select t1.1.1.1;"

# Array with tuple element access (should not add extra parens around array)
format_query "SELECT [['hello']].1;"
