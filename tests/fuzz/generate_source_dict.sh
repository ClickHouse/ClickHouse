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
# function / aggregate function / table function / window function names and
# aliases from their registration code, combinator-expanded aggregate function
# names (sumIf, avgIf, ...) crossing aggregate names with combinator suffixes,
# and data type families and aliases from src/DataTypes, merged with the curated
# tests/fuzz/dictionaries/old.dict. Unlike a committed snapshot, it cannot drift
# from the sources it is generated from.
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

    # Names returned by a getName() accessor that yields a string literal, e.g.
    #     static const char * getName() { return "contingency"; }
    # Such names reach the factory as factory.registerFunction(Data::getName(),
    # ...), so the registration call itself carries no string literal for the
    # pass below to find (contingency, cramersV, cramersVBiasCorrected, theilsU).
    grep -rhozE 'getName\(\)[[:space:]]*\{[[:space:]]*return[[:space:]]+"[^"]+"' \
        "$SOURCE_ROOT/src/Functions" \
        "$SOURCE_ROOT/src/AggregateFunctions" \
        "$SOURCE_ROOT/src/TableFunctions" \
        | tr '\0' '\n' | grep -aoE '"[^"]+"' | tr -d '"'

    # Functions, aliases and data type families registered with a string
    # literal, e.g. factory.registerAlias("SUBSTRING", ...), including calls
    # with a line break before the name, e.g. factory.registerSimpleDataType(
    #     "Date32", ...). Window functions and their aliases (rank, denseRank,
    # row_number, lag, lead, ...) live outside the four directories above, in
    # src/Processors/Transforms/WindowTransform.cpp, so it is scanned too.
    grep -rhozE '(registerFunction|registerAlias|factory\.register[A-Za-z]+)[[:space:]]*\([[:space:]]*"[^"]+"' \
        "$SOURCE_ROOT/src/Functions" \
        "$SOURCE_ROOT/src/AggregateFunctions" \
        "$SOURCE_ROOT/src/TableFunctions" \
        "$SOURCE_ROOT/src/DataTypes" \
        "$SOURCE_ROOT/src/Processors/Transforms/WindowTransform.cpp" \
        | tr '\0' '\n' | grep -aoE '"[^"]+"' | tr -d '"'

    # Combinator-expanded aggregate function names, e.g. sumIf, avgIf,
    # groupArrayArray, uniqState. The authoritative binary path generates these
    # by crossing system.functions (is_aggregate = 1) with
    # system.aggregate_function_combinators; codegen_fuzzer consumes each
    # dictionary row as a single terminal, so the bare names sum and If do not
    # make sumIf reachable - the expanded token has to be emitted explicitly.
    #
    # The combinator suffixes are the names the combinators register themselves
    # under: literal getName() overrides (If, Array, State, ...) and the
    # concatenation forms nested_function->getName() + "Suffix" (ArgMin, ArgMax,
    # OrNull, OrDefault, Resample, ...).
    combinator_suffixes=$(
        {
            grep -rhoE 'String getName\(\) const override[[:space:]]*\{[[:space:]]*return[[:space:]]*"[^"]+"' \
                "$SOURCE_ROOT/src/AggregateFunctions/Combinators"
            grep -rhoE 'getName\(\)[[:space:]]*\+[[:space:]]*"[^"]+"' \
                "$SOURCE_ROOT/src/AggregateFunctions/Combinators"
        } | grep -aoE '"[^"]+"' | tr -d '"' | LC_ALL=C sort -u
    )
    # Base aggregate function names and their aliases (the combinators apply to
    # both), taken from src/AggregateFunctions with the same three passes used
    # above (constexpr name constants such as NameQuantile::name, getName()
    # accessors, and registerFunction/registerAlias calls). The Combinators
    # subdirectory is excluded so the combinator suffixes themselves are not
    # treated as base names.
    aggregate_names=$(
        {
            grep -rhozE --exclude-dir=Combinators \
                'constexpr [a-zA-Z_:<> *]+ [A-Za-z_]*[Nn]ame(\[\])?[[:space:]]*[={][^;]*;' \
                "$SOURCE_ROOT/src/AggregateFunctions"
            grep -rhozE --exclude-dir=Combinators \
                'getName\(\)[[:space:]]*\{[[:space:]]*return[[:space:]]+"[^"]+"' \
                "$SOURCE_ROOT/src/AggregateFunctions"
            grep -rhozE --exclude-dir=Combinators \
                '(registerFunction|registerAlias)[[:space:]]*\([[:space:]]*"[^"]+"' \
                "$SOURCE_ROOT/src/AggregateFunctions"
        } | tr '\0' '\n' | grep -aoE '"[^"]+"' | tr -d '"' \
            | grep -vE '[*?\["\\]' | LC_ALL=C sort -u
    )
    if [ -n "$combinator_suffixes" ] && [ -n "$aggregate_names" ]; then
        while IFS= read -r agg_name; do
            [ -z "$agg_name" ] && continue
            while IFS= read -r suffix; do
                [ -z "$suffix" ] && continue
                echo "${agg_name}${suffix}"
            done <<< "$combinator_suffixes"
        done <<< "$aggregate_names"
    fi
} > "$TMP_FILE"

# Quote the extracted tokens (dropping the rare ones with characters that are
# unsafe for the dictionary format or downstream shell processing), merge with
# the curated dictionary, and deduplicate.
{
    grep -vE '[*?\["\\]' "$TMP_FILE" | sed 's/.*/"&"/'
    cat "$SOURCE_ROOT/tests/fuzz/dictionaries/old.dict"
} | LC_ALL=C sort -u > "$OUTPUT_FILE"

echo "Generated $OUTPUT_FILE: $(wc -l < "$OUTPUT_FILE") tokens"
