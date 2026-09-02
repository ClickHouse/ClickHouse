#!/bin/bash

set -euo pipefail

SCRIPT_DIR=$(dirname "$(realpath "$0")")
ROOT_PATH="$(git rev-parse --show-toplevel)"
CLICKHOUSE_BIN="${CLICKHOUSE_BIN:-$ROOT_PATH/build/programs/clickhouse}"
DICTIONARIES_DIR="$SCRIPT_DIR/dictionaries"

echo "Generating functions dict"
$CLICKHOUSE_BIN local -q "SELECT * FROM (SELECT DISTINCT concat('\"', name, '\"') as res FROM system.functions ORDER BY name UNION ALL SELECT concat('\"', a.name, b.name, '\"') as res FROM system.functions as a CROSS JOIN system.aggregate_function_combinators as b WHERE a.is_aggregate = 1) ORDER BY res" > "$DICTIONARIES_DIR/functions.dict"

echo "Generating data types dict"
$CLICKHOUSE_BIN local -q "SELECT DISTINCT concat('\"', name, '\"') as res FROM system.data_type_families ORDER BY name" > "$DICTIONARIES_DIR/datatypes.dict"

echo "Generating keywords dict"
$CLICKHOUSE_BIN local -q "SELECT DISTINCT concat('\"', keyword, '\"') as res FROM system.keywords ORDER BY keyword" > "$DICTIONARIES_DIR/keywords.dict"

echo "Merging dictionaries into all.dict"
cat "$DICTIONARIES_DIR"/* | LC_ALL=C sort | uniq > "$SCRIPT_DIR/all.dict"