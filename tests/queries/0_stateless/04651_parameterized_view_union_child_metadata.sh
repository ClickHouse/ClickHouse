#!/usr/bin/env bash

# A parameterized view whose SELECT body is not a flat list of ASTSelectQuery children (INTERSECT/
# EXCEPT, or a UNION chain mixing DISTINCT and ALL) used to lose its "parameterized" classification
# once the interpreter rewrote the union list, so its metadata could not be read back: loading it
# threw "Invalid storage definition in metadata file" and aborted the server on startup.
# The second clickhouse-local invocation over the same --path is what exercises the load path.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

dir=${CLICKHOUSE_TEST_UNIQUE_NAME}
[[ -d $dir ]] && rm -rf "$dir"
mkdir "$dir"

echo '--- create ---'
$CLICKHOUSE_LOCAL --path "$dir" --multiline -q """
CREATE DATABASE db ENGINE = Atomic;
CREATE VIEW db.v_intersect AS SELECT {n:UInt64} AS x INTERSECT SELECT 2;
CREATE VIEW db.v_except AS SELECT {n:UInt64} AS x EXCEPT SELECT 2;
CREATE VIEW db.v_nested_union AS SELECT {n:UInt64} AS x UNION DISTINCT SELECT 2 UNION ALL SELECT 3;
CREATE VIEW db.v_flat AS SELECT {n:UInt64} AS x;
CREATE VIEW db.v_not_parameterized AS SELECT 1 AS x INTERSECT SELECT 2;
SELECT name, parameterized_view_parameters FROM system.tables WHERE database = 'db' ORDER BY name;
"""

echo '--- reload ---'
$CLICKHOUSE_LOCAL --path "$dir" -q "SELECT name, parameterized_view_parameters FROM system.tables WHERE database = 'db' ORDER BY name"

echo '--- query after reload ---'
$CLICKHOUSE_LOCAL --path "$dir" --multiline -q """
SELECT * FROM db.v_intersect(n = 2);
SELECT * FROM db.v_except(n = 7);
SELECT * FROM db.v_nested_union(n = 9) ORDER BY x;
SELECT * FROM db.v_flat(n = 5);
SELECT * FROM db.v_not_parameterized;
"""

rm -rf "$dir"

# The explicitly parenthesized nested union is a distinct shape: the parser (not a rewrite pass)
# nests it, so before the fix the parameter was already invisible at CREATE time and the parameter
# got substituted instead of being kept. It never wrote unreadable metadata, but the same accessor
# governs it, so the view is now preserved as parameterized. It needs its own invocation because on
# an unfixed binary the CREATE fails and would abort the script above.
dir2=${CLICKHOUSE_TEST_UNIQUE_NAME}_paren
[[ -d $dir2 ]] && rm -rf "$dir2"
mkdir "$dir2"

echo '--- create (parenthesized) ---'
$CLICKHOUSE_LOCAL --path "$dir2" --multiline -q """
CREATE DATABASE db ENGINE = Atomic;
CREATE VIEW db.v_paren AS SELECT 1 AS x UNION DISTINCT (SELECT {n:UInt64} AS x UNION ALL SELECT 3);
SELECT name, parameterized_view_parameters FROM system.tables WHERE database = 'db' ORDER BY name;
"""

echo '--- reload (parenthesized) ---'
$CLICKHOUSE_LOCAL --path "$dir2" --multiline -q """
SELECT name, parameterized_view_parameters FROM system.tables WHERE database = 'db' ORDER BY name;
SELECT * FROM db.v_paren(n = 8) ORDER BY x;
"""

rm -rf "$dir2"
