#!/usr/bin/env bash

# A parameterized view call is resolved as a `TableFunctionNode`, but unlike a real table function it has a
# name of its own: its columns bind by that name and it needs no alias in a JOIN.
# https://github.com/ClickHouse/ClickHouse/issues/119837

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The fix is in the analyzer and the test pins it: with enable_analyzer = 0 a dotted `db.view(...)` call fails with UNKNOWN_FUNCTION.
CLIENT="$CLICKHOUSE_CLIENT --enable_analyzer=1 --joined_subquery_requires_alias=1"
DB=$CLICKHOUSE_DATABASE

$CLIENT --query "
CREATE TABLE t1 (c1 String, c2 Float64) ENGINE = ReplacingMergeTree ORDER BY c1;
INSERT INTO t1 SELECT toString(number), toFloat64(number) FROM numbers(5);
CREATE TABLE t2 (c1 String, c3 UInt8) ENGINE = MergeTree ORDER BY c1;
INSERT INTO t2 SELECT toString(number), number FROM numbers(3);
CREATE VIEW pv1 AS SELECT {p1:String} AS k1, 'x' AS c1;
CREATE VIEW pv2 AS SELECT arrayJoin({p1:Array(Int64)}) AS k1, 'dev' AS c1;
CREATE VIEW pv3 AS SELECT c1 AS k1, sum(c2) AS m1 FROM t1 WHERE length({p1:Array(String)}) = 0 OR c1 IN ({p1:Array(String)}) GROUP BY c1;
"

echo '-- columns qualified by the view name (issue case 1)'
$CLIENT --query "SELECT pv1.c1 FROM \`$DB.pv1\`(p1 = 'a') LIMIT 1"
$CLIENT --query "SELECT pv1.c1 FROM pv1(p1 = 'a')"
$CLIENT --query "SELECT $DB.pv1.c1 FROM $DB.pv1(p1 = 'a')"

echo '-- qualifier inside IN (issue case 2)'
$CLIENT --query "SELECT c1 FROM \`$DB.pv2\`(p1 = [157]) WHERE pv2.k1 IN (157) LIMIT 100"
$CLIENT --query "SELECT c1 FROM pv2(p1 = [157]) WHERE $DB.pv2.k1 IN (157)"

echo '-- unaliased JOIN operand (issue case 3)'
$CLIENT --query "SELECT $DB.t1.c1, $DB.pv3.k1, $DB.pv3.m1 FROM $DB.t1 FINAL INNER JOIN \`$DB.pv3\`(p1 = []) ON $DB.t1.c1 = $DB.pv3.k1 ORDER BY m1 DESC LIMIT 200"
$CLIENT --query "SELECT count() FROM t1 INNER JOIN pv3(p1 = []) ON t1.c1 = pv3.k1"
$CLIENT --query "SELECT count() FROM t1, pv3(p1 = ['1'])"
$CLIENT --query "SELECT count() FROM pv3(p1 = []) INNER JOIN t1 ON t1.c1 = pv3.k1"

echo '-- qualified matcher and matcher-expanded column names'
$CLIENT --query "SELECT pv3.* FROM t1 INNER JOIN pv3(p1 = []) ON t1.c1 = pv3.k1 ORDER BY k1 LIMIT 1"
# `c1` of the view clashes with `t1.c1`, so the view's copy must be qualified with the view name
$CLIENT --query "SELECT * FROM t1 INNER JOIN pv1(p1 = 'a') ON t1.c1 = pv1.c1 LIMIT 0 SETTINGS analyzer_compatibility_multiple_joins_qualify_column_names = 0 FORMAT TSVWithNames"
$CLIENT --query "SELECT * FROM t1 INNER JOIN pv3(p1 = []) ON t1.c1 = pv3.k1 INNER JOIN t2 ON t1.c1 = t2.c1 LIMIT 0 SETTINGS analyzer_compatibility_multiple_joins_qualify_column_names = 0 FORMAT TSVWithNames"
$CLIENT --query "SELECT * FROM t1 INNER JOIN pv3(p1 = []) ON t1.c1 = pv3.k1 INNER JOIN t2 ON t1.c1 = t2.c1 LIMIT 0 SETTINGS analyzer_compatibility_multiple_joins_qualify_column_names = 1 FORMAT TSVWithNames"
$CLIENT --query "SELECT $DB.pv3.* FROM t1 INNER JOIN pv3(p1 = []) ON t1.c1 = pv3.k1 ORDER BY k1 LIMIT 1"
$CLIENT --query "SELECT count() FROM pv3(p1 = []) INNER JOIN pv3(p1 = ['1']) USING (k1)"

echo '-- arguments still survive per call (issue 112148 must stay fixed)'
$CLIENT --query "SELECT k1 FROM pv3(p1 = ['1'])"
$CLIENT --query "SELECT count() FROM pv3(p1 = []) AS a INNER JOIN pv3(p1 = ['1']) AS b USING (k1)"

echo '-- controls: regular table functions gain nothing'
$CLIENT --query "SELECT numbers.number FROM numbers(3)" 2>&1 | grep -o -m1 'UNKNOWN_IDENTIFIER'
$CLIENT --query "SELECT number FROM numbers(2) AS n INNER JOIN numbers(3) ON 1 = 1" 2>&1 | grep -o -m1 'ALIAS_REQUIRED'
$CLIENT --query "SELECT view.dummy FROM view(SELECT 1 AS dummy)" 2>&1 | grep -o -m1 'UNKNOWN_IDENTIFIER'
$CLIENT --query "SELECT c1 FROM t1 INNER JOIN view(SELECT '1' AS c1) ON 1 = 1" 2>&1 | grep -o -m1 'ALIAS_REQUIRED'
# a clashing column of `view(...)` has no name to be qualified with, in both qualification modes
$CLIENT --query "SELECT * FROM t1, view(SELECT '0' AS c1) LIMIT 0 SETTINGS joined_subquery_requires_alias = 0, analyzer_compatibility_multiple_joins_qualify_column_names = 0 FORMAT TSVWithNames"
$CLIENT --query "SELECT * FROM t1 INNER JOIN t2 ON t1.c1 = t2.c1, view(SELECT '0' AS c1) LIMIT 0 SETTINGS joined_subquery_requires_alias = 0, analyzer_compatibility_multiple_joins_qualify_column_names = 1 FORMAT TSVWithNames"
