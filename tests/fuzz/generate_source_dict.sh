#!/usr/bin/env bash

set -euo pipefail

# The extraction needs `mapfile` (bash 4) and NUL-separated grep output (GNU
# grep -z); stock macOS ships bash 3.2 and BSD grep. Check both up front, so an
# unsupported environment fails here rather than emitting a short dictionary
# whose gaps only surface later as a coverage failure.
if [ "${BASH_VERSINFO[0]}" -lt 4 ]
then
    echo "error: bash 4 or newer is required (found $BASH_VERSION)." \
         "On macOS, install a newer bash and put it ahead of /bin/bash in PATH." >&2
    exit 1
fi
if ! printf 'x' | grep -qzE 'x' 2>/dev/null
then
    echo "error: GNU grep is required (for its -z option)." \
         "On macOS, install GNU grep and put it ahead of BSD grep in PATH." >&2
    exit 1
fi

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
# Names composed at compile time (macro token-pasting like toIntervalDay,
# template concatenation like tuplePlus / emptyArrayUInt8 / L2Normalize /
# inIgnoreSet) cannot be derived from the sources by scanning; they are kept in
# the curated old.dict. update_dict.sh - which runs in the nightly libFuzzer
# job against a real binary - verifies that the output of this script covers
# the binary-derived dictionary, so any gap fails loudly instead of drifting.
#
# Usage: generate_source_dict.sh <clickhouse_source_root> <output_file>

SOURCE_ROOT="$1"
OUTPUT_FILE="$2"

TMP_FILE=$(mktemp)
LABEL_FRAGMENTS_FILE=$(mktemp)
DEAD_CODE_NAMES_FILE=$(mktemp)
INTERNAL_REGISTERED_FILE=$(mktemp)
trap 'rm -f "$TMP_FILE" "$LABEL_FRAGMENTS_FILE" "$DEAD_CODE_NAMES_FILE" "$INTERNAL_REGISTERED_FILE"' EXIT

# The trees holding the registration code: every pass below scans a subset of
# them.
SCANNED_TREES=(
    "$SOURCE_ROOT/src/Functions"
    "$SOURCE_ROOT/src/AggregateFunctions"
    "$SOURCE_ROOT/src/TableFunctions"
    "$SOURCE_ROOT/src/DataTypes"
    "$SOURCE_ROOT/src/Formats"
    "$SOURCE_ROOT/src/Processors/Transforms/WindowTransform.cpp"
    "$SOURCE_ROOT/src/Storages/ObjectStorage/StorageObjectStorageDefinitions.h"
)

# A missing path is a nonzero grep just like an empty match set is, and the
# extractors below have to tell those apart, so require the scanned paths here
# rather than letting a broken checkout reach them.
for scanned_path in "${SCANNED_TREES[@]}" \
    "$SOURCE_ROOT/src/Parsers/CommonParsers.h" \
    "$SOURCE_ROOT/src/AggregateFunctions/Combinators" \
    "$SOURCE_ROOT/tests/fuzz/dictionaries/old.dict"
do
    if [ ! -e "$scanned_path" ]
    then
        echo "error: $scanned_path does not exist." \
             "Pass the root of a ClickHouse source tree." >&2
        exit 1
    fi
done

# Every pass below is optional: a pattern with no carrier in the tree is an empty
# contribution, not a failure. grep reports that as exit 1, which `set -e` would
# otherwise turn into an abort of the whole generator. Absorb exactly that
# status, so a real error (exit 2 and up: an unreadable path, a bad pattern)
# still fails the generator instead of silently shortening the dictionary.
optional_grep()
{
    grep "$@" || [ "$?" = 1 ]
}

# Names carried by code compiled out with #if 0 are not registered by any
# build: src/Functions/trap.cpp - the whole file, including its
# REGISTER_FUNCTION(Trap) - sits inside such a region, so "trap" is not a
# function name in any binary. Collect the string literals of the disabled
# regions, keeping only those that have no live carrier anywhere else in the
# scanned trees (a name that is also defined by live code stays).
mapfile -t DISABLED_FILES < <(grep -rlE '^#if 0' "${SCANNED_TREES[@]}" || true)
if [ "${#DISABLED_FILES[@]}" -gt 0 ]
then
    disabled_literals()
    {
        # $1 = 1 to print the literals inside #if 0 regions, 0 for the rest.
        awk -v inside="$1" '
            /^#if 0([[:space:]]|$)/ { if (depth == 0) { depth = 1; next } }
            depth > 0 && /^#if/ { ++depth }
            depth > 0 && /^#endif/ { if (--depth == 0) next }
            (depth > 0) == (inside == 1)
        ' "${DISABLED_FILES[@]}" | grep -aoE '"[^"]+"' | tr -d '"' | LC_ALL=C sort -u
    }

    DISABLED_EXCLUDES=()
    for disabled_file in "${DISABLED_FILES[@]}"
    do
        DISABLED_EXCLUDES+=("--exclude=$(basename "$disabled_file")")
    done

    LC_ALL=C comm -23 \
        <(disabled_literals 1) \
        <({ disabled_literals 0
            grep -rhoE '"[^"]+"' "${DISABLED_EXCLUDES[@]}" "${SCANNED_TREES[@]}" | tr -d '"'
          } | LC_ALL=C sort -u) \
        > "$DEAD_CODE_NAMES_FILE"
fi

# Names prefixed with __ are internal by convention, and most of them are
# registered in the function factory nevertheless (__bitWrapperFunc,
# __getScalar, __actionName, ... are used by index analysis, scalar subquery
# execution and the planner). Some are not: the implementation classes in
# src/Functions/vectorQuantization.cpp ("__quantizeDistance"),
# src/Functions/productQuantization.cpp ("__productQuantizationDistance") and
# src/Functions/FunctionTopKFilter.cpp ("__topKFilter") are built by query plan
# optimizations directly and have no REGISTER_FUNCTION. Their name constants
# have exactly the shape of a registered carrier, so they are told apart by the
# registration site: keep an internal name only if some file naming it also
# registers something.
optional_grep -rlE '"__[A-Za-z_0-9]+"' "${SCANNED_TREES[@]}" \
    | while IFS= read -r internal_name_file
do
    if grep -qE 'REGISTER_FUNCTION|factory\.register' "$internal_name_file"
    then
        grep -ohE '"__[A-Za-z_0-9]+"' "$internal_name_file" | tr -d '"'
    fi
done | LC_ALL=C sort -u > "$INTERNAL_REGISTERED_FILE"

# Registered function, aggregate function, table function and data type names
# are identifiers. Multi-word tokens come only from the keyword pass (ADD
# COLUMN, ...) and the register-call pass (SQL-compatibility aliases like
# CHAR VARYING), so every name-constant / helper-body scan restricts its
# output to identifier-shaped tokens. This drops unrelated helper constants
# the scans would otherwise match, e.g. the local label
#     constexpr const char * field_name = "<SKIPPED COLUMN>";
# in src/Formats/EscapingRuleUtils.cpp, or "Not implemented" returned by a
# *Name helper body.
identifiers_only()
{
    grep -axE '[A-Za-z_][A-Za-z0-9_]*' || true
}

# Compile-time name *fragments* are identifier-shaped but are not registered
# names themselves:
#     struct LinfLabel { static constexpr auto name = "inf"; };
# in src/Functions/vectorFunctions.cpp is pasted into composed names
# (LinfNorm, LinfDistance, ...) which live in the curated old.dict, together
# with the other compile-time-composed names. Collect the fragments carried
# by *Label structs so the name-constant scans can exclude them.
grep -rhozE 'struct [A-Za-z_0-9]*Label[[:space:]]*\{[^{}]*"[^"]+"[^{}]*\}' \
    "$SOURCE_ROOT/src/Functions" \
    "$SOURCE_ROOT/src/AggregateFunctions" \
    "$SOURCE_ROOT/src/TableFunctions" \
    | tr '\0' '\n' | grep -aoE '"[^"]+"' | tr -d '"' > "$LABEL_FRAGMENTS_FILE" || true

# The distance / norm computation kernels carry fragments the same way:
#     struct CosineDistance { static constexpr auto name = "Cosine"; ... };
# in src/Functions/array/arrayDistance.cpp is pasted into arrayCosineDistance
# (and its siblings in arrayNorm.cpp into arrayL1Norm, ...). A kernel is
# recognizable mechanically: the struct is named <fragment><Suffix> for the
# composition suffix - L1Norm carries "L1", CosineDistance carries "Cosine",
# ZeroTransform in src/Functions/DateTimeTransforms.h carries "Zero" - while
# real name carriers are not: NameEditDistance carries "editDistance",
# L1DistanceTraits carries the complete name "L1Distance".
{ grep -rhozE 'struct [A-Za-z_0-9]+[[:space:]]*(:[[:space:]]*[A-Za-z_0-9]+[[:space:]]*)?\{[^{}"]*static constexpr auto name = "[^"]+";' \
    "$SOURCE_ROOT/src/Functions" \
    "$SOURCE_ROOT/src/AggregateFunctions" \
    "$SOURCE_ROOT/src/TableFunctions" || true; } \
    | while IFS= read -r -d '' kernel_chunk
do
    kernel_struct=$(sed -E 's/^struct ([A-Za-z_0-9]+).*/\1/' <<< "${kernel_chunk//$'\n'/ }")
    kernel_fragment=$(grep -aoE '"[^"]+"' <<< "${kernel_chunk//$'\n'/ }" | head -1 | tr -d '"')
    for kernel_suffix in Distance Norm Transform
    do
        if [ "$kernel_struct" == "${kernel_fragment}${kernel_suffix}" ]
        then
            echo "$kernel_fragment"
        fi
    done
done >> "$LABEL_FRAGMENTS_FILE"

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
    # Besides constexpr, plain const definitions are covered too, including
    # out-of-class template specializations and inline members:
    #     const char * FunctionPolygonsUnion<CartesianPoint>::name = "polygonsUnionCartesian";
    #     static inline const char * name = "widthBucket";
    # src/Formats hosts the name constants of the structureTo*Schema functions,
    # and src/Storages/ObjectStorage/StorageObjectStorageDefinitions.h the names
    # of the object storage and data lake table functions (s3, gcs, iceberg*,
    # deltaLake*, paimon*, ... and their *Cluster variants), so they are scanned
    # as well. Arrays of names are covered too:
    #     constexpr std::array<const char *, 2> names = {"generate_series", "generateSeries"};
    # Every identifier-shaped string literal in the initializer is taken,
    # except the compile-time name fragments collected above. Three declaration
    # shapes carry no registered names and are dropped whole: map-typed lookup
    # tables (their literals are labels keyed by something else, e.g.
    # capnp_simple_type_names in src/Formats/CapnProtoSchema.cpp maps Cap'n
    # Proto type enums to "Data" / "Interface" / "AnyPointer"), initializers
    # composing the name with + (their literals are fragments, e.g.
    # std::string("L") + FuncLabel::name + "Normalize" in
    # src/Functions/vectorFunctions.cpp; the composed names live in the
    # curated old.dict), and std::string_view constants - the factory
    # registration paths take String, which std::string_view does not
    # implicitly convert to, so a string_view name constant is never an SQL
    # registration; the ones in the tree name foreign entities (the
    # AssemblyScript runtime exports "__new" / "__pin" / "__unpin" in
    # src/Functions/UserDefined/UserDefinedWebAssemblyScriptAbi.cpp) or hold
    # display literals (the month_names / day_names tables of dateName in
    # src/Functions/dateName.cpp). The variable must be a bare name / names or
    # a CamelCase *Name(s) trait constant: a snake_case compound qualifies the
    # name with another entity kind, so it does not carry a registered name -
    # e.g. the argument-name tables mandatory_argument_names /
    # optional_argument_names of makeDate* ("year", "month", "fraction", ...)
    # in src/Functions/makeDate.cpp, or the tuple element labels element_names
    # ("min_x", "max_lat", ...) in src/Functions/MVTBoundingBox.cpp.
    optional_grep -rhozE '\bconst(expr)?[[:space:]]+[a-zA-Z_0-9,:<> *]*([^A-Za-z_0-9][Nn]|[A-Za-z0-9]N)ames?(\[\])?[[:space:]]*[={][^;]*;' \
        "$SOURCE_ROOT/src/Functions" \
        "$SOURCE_ROOT/src/AggregateFunctions" \
        "$SOURCE_ROOT/src/TableFunctions" \
        "$SOURCE_ROOT/src/Formats" \
        "$SOURCE_ROOT/src/Storages/ObjectStorage/StorageObjectStorageDefinitions.h" \
        | { grep -zvE '\+|map<|string_view' || true; } \
        | tr '\0' '\n' | optional_grep -aoE '"[^"]+"' | tr -d '"' \
        | identifiers_only | optional_grep -Fxvf "$LABEL_FRAGMENTS_FILE"

    # Names carried by a local String variable instead of a literal argument,
    # e.g. the in/notIn/globalIn/nullIn family (src/Functions/in.cpp):
    #     String full_name_in = "in";
    #     factory.registerFunction(full_name_in, ...);
    # The variable must flow into a register* call in the same file: a *name*
    # local that is never registered carries no registered name, e.g. the dummy
    # column label
    #     String column_name = "c";
    # in src/Functions/FunctionGenerateRandomStructure.cpp.
    optional_grep -rlE '\bString [A-Za-z_]*[Nn]ame[A-Za-z_]*[[:space:]]*=[[:space:]]*"[^"]+"' \
        "$SOURCE_ROOT/src/Functions" \
        "$SOURCE_ROOT/src/AggregateFunctions" \
        "$SOURCE_ROOT/src/TableFunctions" \
        | while IFS= read -r name_local_file
    do
        grep -ohE '\bString [A-Za-z_]*[Nn]ame[A-Za-z_]*[[:space:]]*=[[:space:]]*"[^"]+"' "$name_local_file" \
            | while IFS= read -r name_local_decl
        do
            name_local_var=$(sed -E 's/^String ([A-Za-z_]+).*/\1/' <<< "$name_local_decl")
            if grep -qE "register[A-Za-z]*[[:space:]]*\([[:space:]]*$name_local_var\b" "$name_local_file"
            then
                grep -oE '"[^"]+"' <<< "$name_local_decl" | tr -d '"'
            fi
        done
    done | identifiers_only

    # Names carried by an alias list registered in a loop, e.g.
    #     static const VectorWithMemoryTracking<std::string> aliases = {"groupConcat", "group_concat", "string_agg"};
    # looped over factory.registerAlias(aliases.at(i), ...) in
    # src/AggregateFunctions/AggregateFunctionGroupConcat.cpp.
    optional_grep -rhozE '[Aa]lias(es)?[[:space:]]*=[[:space:]]*\{[^;]*"[^;]*;' \
        "$SOURCE_ROOT/src/Functions" \
        "$SOURCE_ROOT/src/AggregateFunctions" \
        "$SOURCE_ROOT/src/TableFunctions" \
        | tr '\0' '\n' | optional_grep -aoE '"[^"]+"' | tr -d '"' | identifiers_only

    # Names returned by a *Name/getName helper whose body yields string
    # literals, e.g. an accessor
    #     static const char * getName() { return "contingency"; }
    # (such names reach the factory as factory.registerFunction(Data::getName(),
    # ...) - contingency, cramersV, cramersVBiasCorrected, theilsU), or a free
    # helper choosing between literals
    #     const char * mergeTreeAnalyzeIndexFunctionName(bool resolve_by_uuid)
    #     {
    #         if (resolve_by_uuid)
    #             return "mergeTreeAnalyzeIndexesUUID";
    #         else
    #             return "mergeTreeAnalyzeIndexes";
    #     }
    # Every string literal in the (brace-free) function body is taken. The
    # body must open right after the parameter list: virtual `const override`
    # helpers return *display* names, not registered names - getName of
    # internal function classes ("FunctionExpression", "FunctionCapture",
    # "GeneratorJSONPath"), getFactoryName ("FunctionFactory",
    # "AggregateFunctionFactory"), getStorageEngineName ("Loop", "FuzzQuery") -
    # so they are deliberately not matched. The helper's own name must say it
    # returns a *function* name (getName, or a free helper ending in
    # FunctionName): a helper naming another entity kind returns a name from a
    # different namespace, e.g. getDatabaseName of ITableFunction returns the
    # internal pseudo-database "_table_function".
    optional_grep -rhozE '\bget[Nn]ame[[:space:]]*\([^()]*\)[[:space:]]*\{[^{}"]*"[^{}]*\}|\b[A-Za-z_]*[Ff]unction[Nn]ame[[:space:]]*\([^()]*\)[[:space:]]*\{[^{}"]*"[^{}]*\}' \
        "$SOURCE_ROOT/src/Functions" \
        "$SOURCE_ROOT/src/AggregateFunctions" \
        "$SOURCE_ROOT/src/TableFunctions" \
        | tr '\0' '\n' | optional_grep -aoE '"[^"]+"' | tr -d '"' | identifiers_only

    # Functions, aliases and data type families registered with a string
    # literal, e.g. factory.registerAlias("SUBSTRING", ...), including calls
    # with a line break before the name, e.g. factory.registerSimpleDataType(
    #     "Date32", ...). Window functions and their aliases (rank, denseRank,
    # row_number, lag, lead, ...) live outside the four directories above, in
    # src/Processors/Transforms/WindowTransform.cpp, so it is scanned too.
    optional_grep -rhozE '(registerFunction|registerAlias|factory\.register[A-Za-z]+)[[:space:]]*\([[:space:]]*"[^"]+"' \
        "$SOURCE_ROOT/src/Functions" \
        "$SOURCE_ROOT/src/AggregateFunctions" \
        "$SOURCE_ROOT/src/TableFunctions" \
        "$SOURCE_ROOT/src/DataTypes" \
        "$SOURCE_ROOT/src/Processors/Transforms/WindowTransform.cpp" \
        | tr '\0' '\n' | optional_grep -aoE '"[^"]+"' | tr -d '"'

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
            optional_grep -rhoE 'String getName\(\) const override[[:space:]]*\{[[:space:]]*return[[:space:]]*"[^"]+"' \
                "$SOURCE_ROOT/src/AggregateFunctions/Combinators"
            optional_grep -rhoE 'getName\(\)[[:space:]]*\+[[:space:]]*"[^"]+"' \
                "$SOURCE_ROOT/src/AggregateFunctions/Combinators"
        } | optional_grep -aoE '"[^"]+"' | tr -d '"' | identifiers_only | LC_ALL=C sort -u
    )
    # Base aggregate function names and their aliases (the combinators apply to
    # both), taken from src/AggregateFunctions with the same passes used above
    # (const/constexpr name constants such as NameQuantile::name, getName()
    # accessors, registerFunction/registerAlias/registerAliasUnchecked calls,
    # and looped-over alias lists). Window functions (rank, lag, dense_rank,
    # ...) are aggregate functions too - the binary path crosses them with the
    # combinators as well - so their registration site
    # src/Processors/Transforms/WindowTransform.cpp is included. The
    # Combinators subdirectory is excluded so the combinator suffixes
    # themselves are not treated as base names.
    aggregate_names=$(
        {
            optional_grep -rhozE --exclude-dir=Combinators \
                '\bconst(expr)?[[:space:]]+[a-zA-Z_:<> *]*([^A-Za-z_0-9][Nn]|[A-Za-z0-9]N)ame(\[\])?[[:space:]]*[={][^;]*;' \
                "$SOURCE_ROOT/src/AggregateFunctions" \
                | { grep -zvE '\+|map<|string_view' || true; }
            optional_grep -rhozE --exclude-dir=Combinators \
                'getName\(\)[[:space:]]*\{[[:space:]]*return[[:space:]]+"[^"]+"' \
                "$SOURCE_ROOT/src/AggregateFunctions"
            optional_grep -rhozE --exclude-dir=Combinators \
                '(registerFunction|registerAlias[A-Za-z]*)[[:space:]]*\([[:space:]]*"[^"]+"' \
                "$SOURCE_ROOT/src/AggregateFunctions" \
                "$SOURCE_ROOT/src/Processors/Transforms/WindowTransform.cpp"
            optional_grep -rhozE --exclude-dir=Combinators \
                '[Aa]lias(es)?[[:space:]]*=[[:space:]]*\{[^;]*"[^;]*;' \
                "$SOURCE_ROOT/src/AggregateFunctions"
        } | tr '\0' '\n' | optional_grep -aoE '"[^"]+"' | tr -d '"' \
            | identifiers_only | optional_grep -Fxvf "$LABEL_FRAGMENTS_FILE" | LC_ALL=C sort -u
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
# unsafe for the dictionary format or downstream shell processing, the names of
# code compiled out with #if 0, and the internal __ names without a
# registration site), merge with the curated dictionary, and deduplicate.
{
    optional_grep -vE '[*?\["\\]' "$TMP_FILE" \
        | { grep -Fxvf "$DEAD_CODE_NAMES_FILE" || true; } \
        | awk -v registered_file="$INTERNAL_REGISTERED_FILE" '
            BEGIN { while ((getline name < registered_file) > 0) registered[name] }
            !(/^__/ && !($0 in registered))' \
        | sed 's/.*/"&"/'
    cat "$SOURCE_ROOT/tests/fuzz/dictionaries/old.dict"
} | LC_ALL=C sort -u > "$OUTPUT_FILE"

echo "Generated $OUTPUT_FILE: $(wc -l < "$OUTPUT_FILE") tokens"
