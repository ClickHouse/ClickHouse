#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs a Distributed table over `test_cluster_two_shards`, which the fast test
# configuration does not provide.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Fixed values for assertions about formatting only; a real UUID is read from `system.tables`
# wherever the reference has to actually resolve.
FIXED='a1a1a1a1-b2b2-c3c3-d4d4-e5e5e5e5e5e5'
ZERO='00000000-0000-0000-0000-000000000000'

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO src VALUES (1, 111);
    CREATE TABLE other (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO other VALUES (1);
"

U=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'src'")

echo '-- a pinned reference executes'
$CLICKHOUSE_CLIENT -q "SELECT v FROM src UUID '$U'"

echo '-- the formatter emits the clause, including an explicit all-zero UUID'
$CLICKHOUSE_CLIENT -q "SELECT formatQuery('SELECT k FROM src UUID \'$FIXED\'')"
$CLICKHOUSE_CLIENT -q "SELECT formatQuery('SELECT k FROM src UUID \'$ZERO\'')"

echo '-- a multi-table DROP keeps the clause of each element'
$CLICKHOUSE_CLIENT -q "SELECT formatQuery('DROP TABLE src UUID \'$FIXED\', other')"
$CLICKHOUSE_CLIENT -q "SELECT formatQuery('DROP TABLE src UUID \'$ZERO\', other UUID \'$FIXED\'')"

echo '-- a REFRESH ... DEPENDS ON dependency keeps its clause'
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM (SELECT formatQuery('CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 HOUR DEPENDS ON src UUID \'$FIXED\' ENGINE = Memory AS SELECT 1') AS q) WHERE q ILIKE '%DEPENDS ON src UUID \'$FIXED\'%'"

echo '-- a stored view definition is database-qualified and keeps the pin, written either way'
# One conjunction per view: the definition must be BOTH qualified and clause-bearing, so a rewriter
# that declines to qualify the reference fails the assertion instead of satisfying it.
$CLICKHOUSE_CLIENT -q "
    CREATE VIEW v_qualified AS SELECT v FROM ${CLICKHOUSE_DATABASE}.src UUID '$U';
    CREATE VIEW v_unqualified AS SELECT v FROM src UUID '$U';
    SELECT name, position(create_table_query, '${CLICKHOUSE_DATABASE}.src UUID \'$U\'') > 0
    FROM system.tables WHERE database = currentDatabase() AND name IN ('v_qualified', 'v_unqualified') ORDER BY name;
"

# Rebind the name so resolving by name and resolving by UUID reach different tables: from here on
# 111 means the pin was honoured and 222 means the reference fell back to the name.
$CLICKHOUSE_CLIENT -q "
    RENAME TABLE src TO src_pinned;
    CREATE TABLE src (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO src VALUES (1, 222);
"
echo '-- a stored view still reads the pinned table'
$CLICKHOUSE_CLIENT -q "SELECT v FROM v_qualified SETTINGS enable_analyzer = 0"
$CLICKHOUSE_CLIENT -q "SELECT v FROM v_unqualified SETTINGS enable_analyzer = 0"

echo '-- and the pin survives a round trip through the stored definition text'
$CLICKHOUSE_CLIENT -q "
    DETACH TABLE v_qualified; ATTACH TABLE v_qualified;
    DETACH TABLE v_unqualified; ATTACH TABLE v_unqualified;
    SELECT name, position(create_table_query, 'UUID \'$U\'') > 0
    FROM system.tables WHERE database = currentDatabase() AND name IN ('v_qualified', 'v_unqualified') ORDER BY name;
"
$CLICKHOUSE_CLIENT -q "SELECT v FROM v_qualified SETTINGS enable_analyzer = 0"
$CLICKHOUSE_CLIENT -q "SELECT v FROM v_unqualified SETTINGS enable_analyzer = 0"

echo '-- JOIN predicate pushdown rewrites the reference into a subquery carrying the clause'
# Mechanism oracle: the rewritten plan must show the clause, and the un-rewritten one must not, so
# the pair reddens if the rewriter drops the clause AND if the rewriter never runs.
$CLICKHOUSE_CLIENT --enable_analyzer 0 -q "SELECT count() > 0 FROM (EXPLAIN SYNTAX SELECT v FROM other AS o JOIN src UUID '$U' USING (k) SETTINGS enable_optimize_predicate_expression = 1) WHERE explain ILIKE '%FROM src UUID \'$U\'%'"
$CLICKHOUSE_CLIENT --enable_analyzer 0 -q "SELECT count() > 0 FROM (EXPLAIN SYNTAX SELECT v FROM other AS o JOIN src UUID '$U' USING (k) SETTINGS enable_optimize_predicate_expression = 0) WHERE explain ILIKE '%FROM src UUID \'$U\'%'"
echo '-- so the pin is honoured with the pushdown on and off alike'
$CLICKHOUSE_CLIENT -q "SELECT v FROM other AS o JOIN src UUID '$U' USING (k) SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 1"
$CLICKHOUSE_CLIENT -q "SELECT v FROM other AS o JOIN src UUID '$U' USING (k) SETTINGS enable_analyzer = 0, enable_optimize_predicate_expression = 0"

echo '-- renaming a reference to its remote table voids the pin: no clause is emitted'
$CLICKHOUSE_CLIENT -q "CREATE TABLE dist (k UInt64) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), 'src')"
DU=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'dist'")
$CLICKHOUSE_CLIENT --enable_analyzer 0 --distributed_product_mode local \
    -q "EXPLAIN SYNTAX SELECT k FROM dist AS a WHERE k IN (SELECT k FROM dist UUID '$DU' AS b)" | grep -c 'UUID'

echo '-- controls: a clause-free reference and the CREATE forms format as before'
$CLICKHOUSE_CLIENT -q "SELECT formatQuery('SELECT k FROM src')"
$CLICKHOUSE_CLIENT -q "SELECT formatQuery('CREATE TABLE x UUID \'$FIXED\' (k UInt64) ENGINE = MergeTree ORDER BY k')"

echo '-- AST JSON round-trips a user-written clause'
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON(parseQueryToJSON('SELECT k FROM src UUID \'$FIXED\''))"
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON(parseQueryToJSON('SELECT k FROM src UUID \'$ZERO\''))"

echo '-- AST JSON still fails closed for a UUID that no clause would format back'
$CLICKHOUSE_CLIENT -q "SELECT formatQueryFromJSON(replace(parseQueryToJSON('SELECT k FROM src'), '\"type\":\"TableIdentifier\",\"name\":\"src\"', '\"type\":\"TableIdentifier\",\"name\":\"src\",\"uuid\":\"$FIXED\"'))" 2>&1 \
    | grep -qF 'BAD_ARGUMENTS' && echo 'rejected'
