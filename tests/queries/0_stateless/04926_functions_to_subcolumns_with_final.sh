#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# optimize_functions_to_subcolumns is normally skipped under FINAL because reading a subcolumn
# instead of computing the function may change the result for value-combining merge engines
# (SummingMergeTree/GraphiteMergeTree/CoalescingMergeTree). It is safe for engines whose FINAL
# merge only selects whole rows (Replacing/Collapsing/VersionedCollapsing), where the subcolumn of
# the surviving row equals the subcolumn derived from that full row. This test checks that the
# rewrite fires (and gives correct results) for the row-selecting engines under FINAL, and is still
# blocked for SummingMergeTree.

# Reports whether the rewrite happened by inspecting the reconstructed AST: a rewritten query reads
# a subcolumn (m.size0 / m.key_x / n.null), otherwise it still calls the function.
verdict() {
    local label="$1" query="$2"
    local ast
    ast=$($CLICKHOUSE_CLIENT --optimize_functions_to_subcolumns=1 --query "EXPLAIN QUERY TREE dump_ast = 1 $query" | grep -E '^SELECT')
    if echo "$ast" | grep -qE '\.size0|\.key_|\.null'; then
        echo "$label: rewritten"
    else
        echo "$label: not rewritten"
    fi
}

MAP_SETTINGS="map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic'"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_final_repl"
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_final_repl (k UInt32, ver UInt32, m Map(String, UInt64), n Nullable(UInt64))
ENGINE = ReplacingMergeTree(ver) ORDER BY k SETTINGS $MAP_SETTINGS;
INSERT INTO t_final_repl VALUES (1, 1, {'a':1,'b':2}, NULL);
INSERT INTO t_final_repl VALUES (1, 2, {'x':9}, 5);
"
echo "-- ReplacingMergeTree (safe): surviving row is ver=2, m={'x':9}, n=5 --"
verdict "length(m)"  "SELECT length(m) FROM t_final_repl FINAL"
verdict "m['x']"     "SELECT m['x'] FROM t_final_repl FINAL"
verdict "isNull(n)"  "SELECT isNull(n) FROM t_final_repl FINAL"
$CLICKHOUSE_CLIENT --optimize_functions_to_subcolumns=1 --query "
SELECT length(m), m['x'], m['a'], isNull(n) FROM t_final_repl FINAL"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_final_repl"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_final_coll"
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_final_coll (k UInt32, sign Int8, m Map(String, UInt64), n Nullable(UInt64))
ENGINE = CollapsingMergeTree(sign) ORDER BY k SETTINGS $MAP_SETTINGS;
INSERT INTO t_final_coll VALUES (1, 1, {'a':1}, NULL);
INSERT INTO t_final_coll VALUES (1, -1, {'a':1}, NULL);
INSERT INTO t_final_coll VALUES (1, 1, {'x':9,'y':8}, 5);
"
echo "-- CollapsingMergeTree (safe): surviving row is m={'x':9,'y':8}, n=5 --"
verdict "length(m)"  "SELECT length(m) FROM t_final_coll FINAL"
verdict "m['x']"     "SELECT m['x'] FROM t_final_coll FINAL"
verdict "isNull(n)"  "SELECT isNull(n) FROM t_final_coll FINAL"
$CLICKHOUSE_CLIENT --optimize_functions_to_subcolumns=1 --query "
SELECT length(m), m['x'], m['a'], isNull(n) FROM t_final_coll FINAL"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_final_coll"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_final_vcoll"
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_final_vcoll (k UInt32, sign Int8, ver UInt32, m Map(String, UInt64), n Nullable(UInt64))
ENGINE = VersionedCollapsingMergeTree(sign, ver) ORDER BY k SETTINGS $MAP_SETTINGS;
INSERT INTO t_final_vcoll VALUES (1, 1, 1, {'a':1}, NULL);
INSERT INTO t_final_vcoll VALUES (1, -1, 1, {'a':1}, NULL);
INSERT INTO t_final_vcoll VALUES (1, 1, 2, {'p':1,'q':2,'r':3}, 7);
"
echo "-- VersionedCollapsingMergeTree (safe): surviving row is m={'p':1,'q':2,'r':3}, n=7 --"
verdict "length(m)"  "SELECT length(m) FROM t_final_vcoll FINAL"
verdict "m['p']"     "SELECT m['p'] FROM t_final_vcoll FINAL"
verdict "isNull(n)"  "SELECT isNull(n) FROM t_final_vcoll FINAL"
$CLICKHOUSE_CLIENT --optimize_functions_to_subcolumns=1 --query "
SELECT length(m), m['p'], m['a'], isNull(n) FROM t_final_vcoll FINAL"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_final_vcoll"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS t_final_sum"
$CLICKHOUSE_CLIENT --query "
CREATE TABLE t_final_sum (k UInt32, m Map(String, UInt64), v UInt64)
ENGINE = SummingMergeTree ORDER BY k SETTINGS $MAP_SETTINGS;
INSERT INTO t_final_sum VALUES (1, {'a':1,'b':2}, 5);
INSERT INTO t_final_sum VALUES (1, {'a':10,'c':3}, 7);
"
echo "-- SummingMergeTree (unsafe): rewrite blocked under FINAL, allowed without FINAL --"
verdict "length(m) FINAL"    "SELECT length(m) FROM t_final_sum FINAL"
verdict "m['a'] FINAL"       "SELECT m['a'] FROM t_final_sum FINAL"
verdict "length(m) no FINAL" "SELECT length(m) FROM t_final_sum"
verdict "m['a'] no FINAL"    "SELECT m['a'] FROM t_final_sum"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_final_sum"
