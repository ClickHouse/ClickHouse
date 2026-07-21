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

    # Function, aggregate function (incl. combinators) and table function names.
    # The -z (NUL-separated) match spans lines, so besides the common
    #     static constexpr auto name = "plus";
    # it also covers multiline conditional initializers
    #     static constexpr auto name = cond ? "IPv4StringToNum" : "IPv4StringToNumOrNull";
    # and trait name constants referenced as name = Traits::makeDateName:
    #     static constexpr auto makeDateName = "makeDate";
    # Both = and brace initializers are accepted:
    #     static constexpr auto name{"JSONHas"};
    # Every string literal in the initializer is taken.
    grep -rhozE 'constexpr [a-zA-Z_:<> *]+ [A-Za-z_]*[Nn]ame(\[\])?[[:space:]]*[={][^;]*;' \
        "$SOURCE_ROOT/src/Functions" \
        "$SOURCE_ROOT/src/AggregateFunctions" \
        "$SOURCE_ROOT/src/TableFunctions" \
        | tr '\0' '\n' | grep -aoE '"[^"]+"' | tr -d '"'

    # Functions, aliases and data type families registered with a string
    # literal, e.g. factory.registerAlias("SUBSTRING", ...), including calls
    # with a line break before the name, e.g. factory.registerSimpleDataType(
    #     "Date32", ...).
    grep -rhozE '(registerFunction|registerAlias|factory\.register[A-Za-z]+)[[:space:]]*\([[:space:]]*"[^"]+"' \
        "$SOURCE_ROOT/src/Functions" \
        "$SOURCE_ROOT/src/AggregateFunctions" \
        "$SOURCE_ROOT/src/TableFunctions" \
        "$SOURCE_ROOT/src/DataTypes" \
        | tr '\0' '\n' | grep -aoE '"[^"]+"' | tr -d '"'
} > "$TMP_FILE"

# Quote the extracted tokens (dropping the rare ones with characters that are
# unsafe for the dictionary format or downstream shell processing), merge with
# the curated dictionary, and deduplicate.
{
    grep -vE '[*?\["\\]' "$TMP_FILE" | sed 's/.*/"&"/'
    cat "$SOURCE_ROOT/tests/fuzz/dictionaries/old.dict"
} | LC_ALL=C sort -u > "$OUTPUT_FILE"

echo "Generated $OUTPUT_FILE: $(wc -l < "$OUTPUT_FILE") tokens"
