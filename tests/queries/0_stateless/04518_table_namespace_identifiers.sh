#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=$CLICKHOUSE_DATABASE

$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE \`ns.t\` (x UInt32, s String) ENGINE = MergeTree ORDER BY x;
    INSERT INTO \`ns.t\` VALUES (1, 'a'), (2, 'b');
    CREATE TABLE \`ns.inset\` (x UInt32) ENGINE = Memory;
    INSERT INTO \`ns.inset\` VALUES (1), (2);
"

for analyzer in 0 1
do
    echo "-- enable_analyzer = $analyzer"

    echo "-- column qualified by the namespace path"
    $CLICKHOUSE_CLIENT --enable_analyzer=$analyzer -q "SELECT ns.t.x FROM ns.t ORDER BY x"

    echo "-- column qualified by database and namespace path"
    $CLICKHOUSE_CLIENT --enable_analyzer=$analyzer -q "SELECT $db.ns.t.x FROM $db.ns.t ORDER BY x"

    echo "-- qualified asterisk with a namespace path"
    $CLICKHOUSE_CLIENT --enable_analyzer=$analyzer -q "SELECT ns.t.* FROM ns.t ORDER BY x"

    echo "-- qualified asterisk with database and namespace path"
    $CLICKHOUSE_CLIENT --enable_analyzer=$analyzer -q "SELECT $db.ns.t.* FROM $db.ns.t ORDER BY x"

    echo "-- alias wins over the path"
    $CLICKHOUSE_CLIENT --enable_analyzer=$analyzer -q "SELECT a.x FROM $db.ns.t AS a ORDER BY a.x"

    echo "-- parameterized view through a namespace path"
    $CLICKHOUSE_CLIENT -m -q "CREATE VIEW IF NOT EXISTS \`ns.pv\` AS SELECT x FROM \`ns.t\` WHERE x = {p:UInt32}"
    $CLICKHOUSE_CLIENT --enable_analyzer=$analyzer -q "SELECT x FROM ns.pv(p = 2)"
    $CLICKHOUSE_CLIENT --enable_analyzer=$analyzer -m -q "USE $db.ns; SELECT x FROM pv(p = 1)"

    echo "-- IN with a table path on the right-hand side"
    $CLICKHOUSE_CLIENT --enable_analyzer=$analyzer -q "SELECT count() FROM \`ns.t\` WHERE x IN ns.inset"

    echo "-- additional_table_filters matches relative and canonical names"
    $CLICKHOUSE_CLIENT --enable_analyzer=$analyzer -m -q "USE $db.ns; SELECT count() FROM t SETTINGS additional_table_filters = {'t': 'x = 1'}"
    $CLICKHOUSE_CLIENT --enable_analyzer=$analyzer -m -q "USE $db.ns; SELECT count() FROM t SETTINGS additional_table_filters = {'ns.t': 'x = 1'}"

    echo "-- join of two namespace tables"
    $CLICKHOUSE_CLIENT --enable_analyzer=$analyzer -m -q "
        CREATE TABLE IF NOT EXISTS \`ns.u\` (x UInt32, v UInt32) ENGINE = Memory;
        TRUNCATE TABLE \`ns.u\`;
        INSERT INTO \`ns.u\` VALUES (1, 10), (2, 20);
        SELECT ns.t.x, ns.u.v FROM ns.t JOIN ns.u ON ns.t.x = ns.u.x ORDER BY ns.t.x;
    "
done

echo "-- identifiers from query parameters"
$CLICKHOUSE_CLIENT --param_d="$db" --param_n="ns" --param_t="t" \
    -q "SELECT count() FROM {d:Identifier}.{n:Identifier}.{t:Identifier}"
$CLICKHOUSE_CLIENT --param_n="ns" --param_t="t" \
    -q "SELECT count() FROM {n:Identifier}.{t:Identifier}"
$CLICKHOUSE_CLIENT --param_d="$db" --param_n="ns" --param_t="created_p" \
    -q "CREATE TABLE {d:Identifier}.{n:Identifier}.{t:Identifier} (x Int8) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE $db.\`ns.created_p\`"
$CLICKHOUSE_CLIENT --param_d="$db" --param_n="ns" --param_t="created_p" \
    -q "INSERT INTO {d:Identifier}.{n:Identifier}.{t:Identifier} VALUES (9)"
$CLICKHOUSE_CLIENT -q "SELECT x FROM $db.\`ns.created_p\`"
$CLICKHOUSE_CLIENT --param_d="$db" --param_n="ns" -q "SHOW TABLES FROM {d:Identifier}.{n:Identifier} LIKE 'created%'"

echo "-- a substituted component with a literal dot is rejected"
$CLICKHOUSE_CLIENT --param_n="a.b" --param_t="t" -q "SELECT count() FROM $db.{n:Identifier}.{t:Identifier}" 2>&1 | grep -m1 -c "BAD_QUERY_PARAMETER"
$CLICKHOUSE_CLIENT --param_n="a.b" --param_t="t" -q "INSERT INTO $db.{n:Identifier}.{t:Identifier} VALUES (1)" 2>&1 | grep -m1 -c "BAD_QUERY_PARAMETER"

echo "-- joinGet with a namespace-path table string"
$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE \`ns.jt\` (k UInt32, v String) ENGINE = Join(ANY, LEFT, k);
    INSERT INTO \`ns.jt\` VALUES (1, 'one');
    SELECT joinGet('ns.jt', 'v', toUInt32(1));
"

echo "-- joinGet dependency tracking protects the actual Join table"
$CLICKHOUSE_CLIENT -q "CREATE VIEW dep_view AS SELECT joinGet('ns.jt', 'v', toUInt32(1)) AS r"
$CLICKHOUSE_CLIENT -q "DROP TABLE \`ns.jt\` SETTINGS check_referential_table_dependencies = 1" 2>&1 | grep -m1 -c "HAVE_DEPENDENT_OBJECTS"

echo "-- loop table function under the namespace"
$CLICKHOUSE_CLIENT -m -q "USE $db.ns; SELECT x FROM loop(t) LIMIT 1"

echo "-- fully qualified namespace paths in joinGet and loop"
$CLICKHOUSE_CLIENT -q "SELECT joinGet('$db.ns.jt', 'v', toUInt32(1))"
$CLICKHOUSE_CLIENT -q "SELECT x FROM loop($db.ns.t) LIMIT 1"

echo "-- a fully qualified IN table path works in the new analyzer"
$CLICKHOUSE_CLIENT --enable_analyzer=1 -q "SELECT count() FROM \`ns.t\` WHERE x IN $db.ns.inset"

echo "-- SHOW COLUMNS with a separate namespaced FROM"
$CLICKHOUSE_CLIENT -q "SHOW COLUMNS FROM t FROM $db.ns" | wc -l

echo "-- UNDROP parses a multipart table path"
$CLICKHOUSE_CLIENT -q "SELECT formatQuery('UNDROP TABLE a.b.c')"

echo "-- JSON subcolumn delimiters are untouched in column context"
$CLICKHOUSE_CLIENT -m -q "
    SET enable_json_type = 1;
    CREATE TABLE \`ns.j\` (json JSON) ENGINE = Memory;
    INSERT INTO \`ns.j\` VALUES ('{\"a\": 7}');
    SELECT json.a.:Int64 FROM ns.j;
"

echo "-- JSON subcolumn delimiters are not part of a table path"
$CLICKHOUSE_CLIENT -q "SELECT * FROM ns.j.:x" 2>&1 | grep -m1 -c "Syntax error"
