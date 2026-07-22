#!/bin/bash

set -euo pipefail

# Generate the libFuzzer dictionary (all.dict) from a ClickHouse binary.
#
# The dictionary lists every function, data type family and keyword known to the
# server. Deriving it from the binary - instead of committing a snapshot that
# has to be refreshed by hand - guarantees it never drifts from the actual SQL
# grammar. In CI this runs in the libFuzzer test job against the release binary,
# see ci/jobs/libfuzzer_test_check.py.
#
# Environment variables:
#   CLICKHOUSE_BIN - path to the clickhouse binary (default: the local build).
#   OUTPUT_DIR     - directory to write all.dict into (default: this directory).

SCRIPT_DIR=$(dirname "$(realpath "$0")")
CLICKHOUSE_BIN="${CLICKHOUSE_BIN:-$SCRIPT_DIR/../../build/programs/clickhouse}"
OUTPUT_DIR="${OUTPUT_DIR:-$SCRIPT_DIR}"

# Curated tokens that cannot be derived from the system tables (multi-word
# keywords, historical names, etc.). This is the only committed dictionary.
CURATED_DICT="$SCRIPT_DIR/dictionaries/old.dict"

TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

echo "Using ClickHouse binary: $CLICKHOUSE_BIN"

echo "Generating functions dict"
"$CLICKHOUSE_BIN" local -q "SELECT * FROM (SELECT DISTINCT concat('\"', name, '\"') as res FROM system.functions ORDER BY name UNION ALL SELECT concat('\"', a.name, b.name, '\"') as res FROM system.functions as a CROSS JOIN system.aggregate_function_combinators as b WHERE a.is_aggregate = 1) ORDER BY res" > "$TMP_DIR/functions.dict"

echo "Generating data types dict"
"$CLICKHOUSE_BIN" local -q "SELECT DISTINCT concat('\"', name, '\"') as res FROM system.data_type_families ORDER BY name" > "$TMP_DIR/datatypes.dict"

echo "Generating keywords dict"
"$CLICKHOUSE_BIN" local -q "SELECT DISTINCT concat('\"', keyword, '\"') as res FROM system.keywords ORDER BY keyword" > "$TMP_DIR/keywords.dict"

echo "Merging dictionaries into $OUTPUT_DIR/all.dict"
mkdir -p "$OUTPUT_DIR"
cat "$TMP_DIR"/*.dict "$CURATED_DICT" | LC_ALL=C sort | uniq > "$OUTPUT_DIR/all.dict"

# The source-derived dictionary (generate_source_dict.sh) serves the consumers
# that cannot run a binary: the build-time codegen_select_fuzzer grammar and
# direct users of the fuzzers build output (local runs, OSS-Fuzz). Verify here,
# where a binary exists, that it covers the authoritative binary-derived
# surface, so extractor gaps cannot regress silently.
echo "Checking that the source-derived dictionary covers the binary-derived one"
"$SCRIPT_DIR/generate_source_dict.sh" "$SCRIPT_DIR/../.." "$TMP_DIR/source.dict"
MISSING_TOKENS=$(comm -23 <(LC_ALL=C sort -u "$OUTPUT_DIR/all.dict") <(LC_ALL=C sort -u "$TMP_DIR/source.dict"))
if [ -n "$MISSING_TOKENS" ]; then
    echo "error: tokens present in the binary-derived all.dict are missing from the source-derived dictionary:"
    echo "$MISSING_TOKENS"
    echo "Extend tests/fuzz/generate_source_dict.sh to extract them, or - if a name is composed at" \
         "compile time and cannot be derived from the sources - add it to tests/fuzz/dictionaries/old.dict."
    exit 1
fi
