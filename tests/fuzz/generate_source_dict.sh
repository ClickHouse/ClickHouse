#!/bin/bash

set -euo pipefail

# Generate a libFuzzer dictionary from the ClickHouse *sources*, without running
# a binary.
#
# The authoritative dictionary (all.dict) is produced by update_dict.sh from a
# release binary at fuzzing time, but two consumers cannot use it:
#   - the codegen_select_fuzzer grammar is embedded at build time, when no
#     runnable binary exists yet;
#   - the fuzzers build output ships .options files referencing all.dict, and
#     direct consumers of that output (local runs, OSS-Fuzz) have no
#     regeneration step.
# For them this script derives a best-effort token set from the source tree:
# parser keywords from CommonParsers.h (the source of system.keywords),
# function / aggregate function / table function names and aliases from their
# registration code, data type families and aliases from src/DataTypes, merged
# with the curated tests/fuzz/dictionaries/old.dict. Unlike a committed
# snapshot, it cannot drift from the sources it is generated from.
#
# Usage: generate_source_dict.sh <clickhouse_source_root> <output_file>

SOURCE_ROOT="$1"
OUTPUT_FILE="$2"

TMP_FILE=$(mktemp)
trap 'rm -f "$TMP_FILE"' EXIT

{
    # Parser keywords, e.g. MR_MACROS(ADD_COLUMN, "ADD COLUMN").
    sed -n 's/.*MR_MACROS([A-Z_0-9]*, *"\([^"]*\)").*/\1/p' \
        "$SOURCE_ROOT/src/Parsers/CommonParsers.h"

    # Function, aggregate function (incl. combinators) and table function names,
    # declared as: static constexpr auto name = "plus";
    grep -rhoE 'static constexpr [a-zA-Z_:<> *]+ name(\[\])? *= *"[^"]+"' \
        "$SOURCE_ROOT/src/Functions" \
        "$SOURCE_ROOT/src/AggregateFunctions" \
        "$SOURCE_ROOT/src/TableFunctions" \
        | sed -E 's/.*"([^"]+)".*/\1/'

    # Functions and aliases registered with a string literal,
    # e.g. factory.registerAlias("SUBSTRING", ...).
    grep -rhoE '(registerFunction|registerAlias)\("[^"]+"' \
        "$SOURCE_ROOT/src/Functions" \
        "$SOURCE_ROOT/src/AggregateFunctions" \
        "$SOURCE_ROOT/src/TableFunctions" \
        | sed -E 's/.*"([^"]+)".*/\1/'

    # Data type families and their aliases, e.g. factory.registerDataType("Array", ...).
    grep -rhoE 'factory\.register[A-Za-z]+\("[^"]+"' \
        "$SOURCE_ROOT/src/DataTypes" \
        | sed -E 's/.*"([^"]+)".*/\1/'
} > "$TMP_FILE"

# Quote the extracted tokens (dropping the rare ones with characters that are
# unsafe for the dictionary format or downstream shell processing), merge with
# the curated dictionary, and deduplicate.
{
    grep -vE '[*?\["\\]' "$TMP_FILE" | sed 's/.*/"&"/'
    cat "$SOURCE_ROOT/tests/fuzz/dictionaries/old.dict"
} | LC_ALL=C sort -u > "$OUTPUT_FILE"

echo "Generated $OUTPUT_FILE: $(wc -l < "$OUTPUT_FILE") tokens"
