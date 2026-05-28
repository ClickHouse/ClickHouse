#!/usr/bin/env bash

# For code formatting we have clang-format.
#
# But it's not sane to apply clang-format for whole code base,
#  because it sometimes makes worse for properly formatted files.
#
# It's only reasonable to blindly apply clang-format only in cases
#  when the code is likely to be out of style.
#
# For this purpose we have a script that will use very primitive heuristics
#  (simple regexps) to check if the code is likely to have basic style violations.
#  and then to run formatter only for the specified files.

LC_ALL="en_US.UTF-8"
ROOT_PATH=$(git rev-parse --show-toplevel)
EXCLUDE='build/|integration/|widechar_width/|glibc-compatibility/|poco/|memcpy/|consistent-hashing|benchmark|tests/.*\.cpp$|programs/keeper-bench/example\.yaml|base/base/openpty\.h|src/Storages/ObjectStorage/DataLakes/Iceberg/AvroSchema\.h'
EXCLUDE_DOCS='Settings\.cpp|FormatFactorySettings\.h'

# Pre-compute file lists to avoid repeated find+grep
STYLE_TMPDIR=$(mktemp -d)
trap 'rm -rf "$STYLE_TMPDIR"' EXIT

# All dirs (src,base,programs,utils), h+cpp, with EXCLUDE filter
find $ROOT_PATH/{src,base,programs,utils} -name '*.h' -or -name '*.cpp' 2>/dev/null | grep -vP $EXCLUDE > "$STYLE_TMPDIR/all_excluded"
# Without base dir, h+cpp, with EXCLUDE filter
grep -v '/base/' "$STYLE_TMPDIR/all_excluded" > "$STYLE_TMPDIR/nobase_excluded"
# Without base dir, headers only, with EXCLUDE filter
grep '\.h$' "$STYLE_TMPDIR/nobase_excluded" > "$STYLE_TMPDIR/nobase_headers_excluded"
# Without base dir, h+cpp, without EXCLUDE filter
find $ROOT_PATH/{src,programs,utils} -name '*.h' -or -name '*.cpp' 2>/dev/null > "$STYLE_TMPDIR/nobase_all"
# src+base only, h+cpp, with EXCLUDE filter
grep -v -e '/programs/' -e '/utils/' "$STYLE_TMPDIR/all_excluded" > "$STYLE_TMPDIR/srcbase_excluded"

# All checks are independent — run them in parallel, collecting output in numbered files.
O="$STYLE_TMPDIR/out"

# 01: Style formatting (uses rg native file discovery; tabs and trailing whitespace are checked separately in 02/06b)
{
rg $@ -n --glob '*.h' --glob '*.cpp' \
    --glob '!**/build/**' --glob '!**/integration/**' --glob '!**/widechar_width/**' \
    --glob '!**/glibc-compatibility/**' --glob '!**/poco/**' --glob '!**/memcpy/**' \
    --glob '!**/consistent-hashing/**' --glob '!**/*benchmark*' \
    --glob '!**/tests/**/*.cpp' \
    --glob '!**/base/base/openpty.h' --glob '!**/AvroSchema.h' \
    --glob '!**/*Settings.cpp' --glob '!**/FormatFactorySettings.h' \
    --glob '!**/StorageSystemDashboards.cpp' \
    '((\b(class|struct|namespace|enum|if|for|while|else|throw|switch)\b.*|\)(\s*const)?(\s*noexcept)?(\s*override)?\s*))\{$|^ {1,3}[^\* ]\S|^\s*\b(if|else if|if constexpr|else if constexpr|for|while|catch|switch)\b\(|\( [^\s\\]|\S \)' \
    $ROOT_PATH/{src,base,programs,utils} |
# a curly brace not in a new line, but not for the case of C++11 init or agg. initialization | number of ws not a multiple of 4, but not in the case of comment continuation | missing whitespace after for/if/while... before opening brace | whitespaces inside braces
    rg -v '//|\s+\*|\$\(\(| \)"' && echo "^ style error on this line"
# single-line comment | continuation of a multiline comment | a typical piece of embedded shell code | something like ending of raw string literal
} > "$O.01" 2>&1 &

# 02: Tabs and namespace comments
{
xargs < "$STYLE_TMPDIR/all_excluded" rg $@ -F $'\t' && echo '^ tabs are not allowed'

# // namespace comments are unneeded
result=$(xargs < "$STYLE_TMPDIR/all_excluded" rg $@ '}\s*//+\s*namespace\s*' 2>/dev/null)
if [ -n "$result" ]; then
    echo "$result"
    echo "^ Found unnecessary namespace comments"
fi
} > "$O.02" 2>&1 &

# 03: Duplicated or incorrect setting declarations
bash $ROOT_PATH/ci/jobs/scripts/check_style/check-settings-style > "$O.03" 2>&1 &

# 04: Unused/Undefined/Duplicates ErrorCodes/ProfileEvents/CurrentMetrics
{
EXTERN_TYPES_EXCLUDES=(
    ProfileEvents::global_counters
    ProfileEvents::Event
    ProfileEvents::Count
    ProfileEvents::Counters
    ProfileEvents::end
    ProfileEvents::increment
    ProfileEvents::incrementNoTrace
    ProfileEvents::incrementForLogMessage
    ProfileEvents::incrementLoggerElapsedNanoseconds
    ProfileEvents::getName
    ProfileEvents::Timer
    ProfileEvents::Type
    ProfileEvents::TypeEnum
    ProfileEvents::ValueType
    ProfileEvents::dumpToMapColumn
    ProfileEvents::getProfileEvents
    ProfileEvents::ThreadIdToCountersSnapshot
    ProfileEvents::LOCAL_NAME
    ProfileEvents::keeper_profile_events
    ProfileEvents::CountersIncrement
    ProfileEvents::size
    ProfileEvents::checkCPUOverload
    ProfileEvents::getDocumentation
    ProfileEvents::NAME

    CurrentMetrics::add
    CurrentMetrics::max
    CurrentMetrics::sub
    CurrentMetrics::get
    CurrentMetrics::set
    CurrentMetrics::cas
    CurrentMetrics::getDocumentation
    CurrentMetrics::getName
    CurrentMetrics::end
    CurrentMetrics::Increment
    CurrentMetrics::Metric
    CurrentMetrics::values
    CurrentMetrics::Value
    CurrentMetrics::keeper_metrics
    CurrentMetrics::size

    ErrorCodes::ErrorCode
    ErrorCodes::getName
    ErrorCodes::increment
    ErrorCodes::end
    ErrorCodes::extendedMessage
    ErrorCodes::values
    ErrorCodes::values[i]
    ErrorCodes::getErrorCodeByName
    ErrorCodes::Value
)
# Check unused/undefined/duplicate ErrorCodes, ProfileEvents, CurrentMetrics declarations.
# NOTE: the unused check is pretty dumb and distinguishes only by the type_of_extern,
# and this matches with zkutil::CreateMode
grep -v -e 'src/Common/ZooKeeper/Types.h' -e 'src/Coordination/KeeperConstants.cpp' "$STYLE_TMPDIR/all_excluded" > "$STYLE_TMPDIR/extern_files"

# Extract declarations: "filepath:D TYPE NAME"
xargs < "$STYLE_TMPDIR/extern_files" rg -o --no-line-number \
    'extern const (int|Event|Metric) ([_A-Za-z0-9]+);' -r 'D $1 $2' > "$STYLE_TMPDIR/extern_combined"

# Extract usages (skipping comment lines): "filepath:U NS NAME"
xargs < "$STYLE_TMPDIR/extern_files" rg --no-line-number \
    '(ErrorCodes|ProfileEvents|CurrentMetrics)::[_A-Za-z0-9]+' | \
    awk -F: '{
        file = $1
        line = ""
        for (i = 2; i <= NF; i++) line = line (i > 2 ? ":" : "") $i
        sub(/^[[:space:]]+/, "", line)
        if (substr(line, 1, 2) == "//") next
        while (match(line, /(ErrorCodes|ProfileEvents|CurrentMetrics)::[_A-Za-z0-9]+/)) {
            ns = substr(line, RSTART, RLENGTH)
            sep = index(ns, "::")
            print file ":U " substr(ns, 1, sep - 1) " " substr(ns, sep + 2)
            line = substr(line, RSTART + RLENGTH)
        }
    }' >> "$STYLE_TMPDIR/extern_combined"

# Compare declarations vs usages per file
awk -v excludes="${EXTERN_TYPES_EXCLUDES[*]}" '
BEGIN {
    split(excludes, exc_arr, " ")
    for (i in exc_arr) exc_set[exc_arr[i]] = 1
    type_to_ns["int"] = "ErrorCodes"
    type_to_ns["Event"] = "ProfileEvents"
    type_to_ns["Metric"] = "CurrentMetrics"
}
{
    colon = index($0, ":")
    file = substr($0, 1, colon - 1)
    rest = substr($0, colon + 1)
    split(rest, p, " ")
    if (p[1] == "D") {
        ns = type_to_ns[p[2]]
        key = file SUBSEP ns SUBSEP p[3]
        decl[key]++
    } else {
        key = file SUBSEP p[2] SUBSEP p[3]
        used[key] = 1
    }
}
END {
    for (key in decl) {
        split(key, k, SUBSEP)
        file = k[1]; ns = k[2]; name = k[3]
        if (!(key in used))
            if (!(ns == "ProfileEvents" && substr(name, 1, 4) == "Perf"))
                print ns "::" name " is defined but not used in file " file
        if (decl[key] > 1)
            print "Duplicate " ns " in file " file
    }
    for (key in used) {
        if (!(key in decl)) {
            split(key, k, SUBSEP)
            if (!((k[2] "::" k[3]) in exc_set))
                print k[2] "::" k[3] " is used in file " k[1] " but not defined"
        }
    }
}
' "$STYLE_TMPDIR/extern_combined"
} > "$O.04" 2>&1 &

# 05: Consecutive empty lines and pragma once
{
# Three or more consecutive empty lines (pre-filter with grep to avoid reading all files through awk)
xargs < "$STYLE_TMPDIR/all_excluded" rg -l0 --multiline '\A\n\n\n|\n\n\n\n' 2>/dev/null | \
    xargs -0 awk 'FNR==1 { i = 0 } /^$/ { ++i; if (i > 2) { print "More than two consecutive empty lines in file " FILENAME } } /./ { i = 0 }'

# Check that every header file has #pragma once in first line
xargs < "$STYLE_TMPDIR/nobase_headers_excluded" awk 'FNR==1 && !/^#pragma once$/ { print "File " FILENAME " must have '"'"'#pragma once'"'"' in first line" }'
} > "$O.05" 2>&1 &

# 06a: Too many exclamation marks
{
xargs < "$STYLE_TMPDIR/all_excluded" grep -F '!!!' | grep . && echo "Too many exclamation marks (looks dirty, unconfident)."

# Exclamation mark in a message
xargs < "$STYLE_TMPDIR/all_excluded" grep -F '!",' | grep . && echo "No need for an exclamation mark (looks dirty, unconfident)."
} > "$O.06a" 2>&1 &

# 06b: Trailing whitespaces
{
xargs < "$STYLE_TMPDIR/all_excluded" grep -n ' $' | grep . && echo "^ Trailing whitespaces."
} > "$O.06b" 2>&1 &

# 07a: Forbidden patterns in nobase_excluded (part 1)
{
# Forbid stringstream because it's easy to use them incorrectly and hard to debug possible issues
xargs < "$STYLE_TMPDIR/nobase_excluded" rg 'std::[io]?stringstream' | grep -v "STYLE_CHECK_ALLOW_STD_STRING_STREAM" && echo "Use WriteBufferFromOwnString or ReadBufferFromString instead of std::stringstream"

# Forbid hardware_destructive_interference_size because it provides unrealistic values for ARM (see https://github.com/ClickHouse/ClickHouse/pull/97357)
xargs < "$STYLE_TMPDIR/nobase_excluded" rg '(hardware_destructive_interference_size|hardware_constructive_interference_size)' | grep -vE ':[[:space:]]*//' && echo "Use CH_CACHE_LINE_SIZE from Common/CacheLine.h instead"

# Forbid std::filesystem::is_symlink and std::filesystem::read_symlink, because it's easy to use them incorrectly
xargs < "$STYLE_TMPDIR/nobase_excluded" rg '::(is|read)_symlink' | grep -v "STYLE_CHECK_ALLOW_STD_FS_SYMLINK" && echo "Use DB::FS::isSymlink and DB::FS::readSymlink instead"

# Forbid using std::shared_mutex and point to the faster alternative
xargs < "$STYLE_TMPDIR/nobase_excluded" grep 'std::shared_mutex' | \
  xargs -I{} echo "Found std::shared_mutex '{}'. Please use DB::SharedMutex instead"
} > "$O.07a" 2>&1 &

# 07b: Forbidden patterns in nobase_excluded (part 2)
{
# Forbid __builtin_unreachable(), because it's hard to debug when it becomes reachable
xargs < "$STYLE_TMPDIR/nobase_excluded" grep -F '__builtin_unreachable' && echo "Use UNREACHABLE() from defines.h instead"

# Forbid mt19937() and random_device() which are outdated and slow
xargs < "$STYLE_TMPDIR/nobase_excluded" rg '(std::mt19937|std::mersenne_twister_engine|std::random_device)' && echo "Use pcg64_fast (from pcg_random.h) and randomSeed (from Common/randomSeed.h) instead"

# Require checking return value of close(),
# since it can hide fd misuse and break other places.
xargs < "$STYLE_TMPDIR/nobase_excluded" rg -e ' close\(.*fd' -e ' ::close\(' | grep -v = && echo "Return value of close() should be checked"
} > "$O.07b" 2>&1 &

# 08: std containers lint
{
directories_to_lint_std_containers_usages=(
    src/AggregateFunctions
    src/Columns
    src/Dictionaries
)

for dir in "${directories_to_lint_std_containers_usages[@]}"; do
    grep "/$dir/" "$STYLE_TMPDIR/all_excluded" |
        xargs rg -Hn 'std::(deque|list|map|multimap|multiset|queue|set|unordered_map|unordered_multimap|unordered_multiset|unordered_set|vector)<' |
        grep -v "STYLE_CHECK_ALLOW_STD_CONTAINERS" && echo "Use an -WithMemoryTracking alternative or mark these usages with STYLE_CHECK_ALLOW_STD_CONTAINERS"
done
} > "$O.08" 2>&1 &

# 09: Forbid std::cerr/std::cout in src (fine in programs/utils)
{
std_cerr_cout_excludes=(
    /examples/
    /tests/
    _fuzzer
    # only under #ifdef DBMS_HASH_MAP_DEBUG_RESIZES, that is used only in tests
    src/Common/HashTable/HashTable.h
    # SensitiveDataMasker::printStats()
    src/Common/SensitiveDataMasker.cpp
    # StreamStatistics::print()
    src/Compression/LZ4_decompress_faster.cpp
    # ContextSharedPart with subsequent std::terminate()
    src/Interpreters/Context.cpp
    # IProcessor::dump()
    src/Processors/IProcessor.cpp
    src/Client/ClientApplicationBase.cpp
    src/Common/ProgressIndication.h
    src/Client/LineReader.h
    src/Client/ReplxxLineReader.h
    src/Client/Suggest.cpp
    src/Client/ClientBase.h
    src/Daemon/BaseDaemon.cpp
    src/Loggers/Loggers.cpp
    src/IO/Ask.cpp
    # Only in block comments (/* ... */)
    src/Storages/IStorage.h
    src/Common/mysqlxx/mysqlxx/Query.h
    src/Common/OptimizedRegularExpression.cpp
)
grep -F -v $(printf -- "-e %s " "${std_cerr_cout_excludes[@]}") "$STYLE_TMPDIR/srcbase_excluded" | \
    xargs grep -F -l -e 'std::cerr' -e 'std::cout' | \
    xargs grep -P -l '^\s*(?!//)([^/]|/[^/])*std::c(err|out)' | \
    while read -r src; do echo "$src: uses std::cerr/std::cout"; done
} > "$O.09" 2>&1 &

# 10: Expect test validation (single awk pass instead of per-file grep loop)
{
find $ROOT_PATH/tests/queries -name '*.expect' -print0 | xargs -0 awk '
FNR == 1 {
    if (file != "") check_file()
    file = FILENAME
    debuglog = 0; spawn_client = 0; history = 0; timeout_found = 0; eof_found = 0
}
/^exp_internal -f \$CLICKHOUSE_TMP\/\$basename\.debuglog 0$/ { debuglog = 1 }
/^spawn.*CLICKHOUSE_CLIENT_BINARY$/ { spawn_client = 1 }
/^spawn.*CLICKHOUSE_CLIENT_BINARY.*--history_file/ { history = 1 }
/-i \$any_spawn_id timeout/ { timeout_found = 1 }
/-i \$any_spawn_id eof/ { eof_found = 1 }
END { if (file != "") check_file() }
function check_file() {
    q = "\047"
    if (!debuglog) print "Missing " q "^exp_internal -f $CLICKHOUSE_TMP/$basename.debuglog 0$" q " in " q file q
    if (spawn_client && !history) print "Missing " q "^spawn.*CLICKHOUSE_CLIENT_BINARY.*--history_file$" q " in " q file q
    if (!timeout_found) print "Missing " q "-i $any_spawn_id timeout" q " in " q file q
    if (!eof_found) print "Missing " q "-i $any_spawn_id eof" q " in " q file q
}
'
} > "$O.10" 2>&1 &

# 11: Misc small checks (error codes, find_path, CMake, allow_ settings)
{
# Forbid non-unique error codes
if [[ "$(grep -Po "M\([0-9]*," $ROOT_PATH/src/Common/ErrorCodes.cpp | wc -l)" != "$(grep -Po "M\([0-9]*," $ROOT_PATH/src/Common/ErrorCodes.cpp | sort | uniq | wc -l)" ]]
then
    echo "ErrorCodes.cpp contains non-unique error codes"
fi

# Check that there is no system-wide libraries/headers in use.
#
# NOTE: it is better to override find_path/find_library in cmake, but right now
# it is not possible, see [1] for the reference.
#
#   [1]: git grep --recurse-submodules -e find_library -e find_path contrib
if git grep -e find_path -e find_library -- :**CMakeLists.txt; then
    echo "There is find_path/find_library usage. ClickHouse should use everything bundled. Consider adding one more contrib module."
fi

# Don't allow dynamic compiler check with CMake, because we are using hermetic, reproducible, cross-compiled, static (TLDR, good) builds.
find $ROOT_PATH/contrib/*-cmake -name 'CMakeLists.txt' -or -name '*.cmake' | xargs grep --with-filename -i -E 'check_c_compiler_flag|check_cxx_compiler_flag|check_c_source_compiles|check_cxx_source_compiles|check_include_file|check_symbol_exists|cmake_push_check_state|cmake_pop_check_state|find_package|CMAKE_REQUIRED_FLAGS|CheckIncludeFile|CheckCCompilerFlag|CheckCXXCompilerFlag|CheckCSourceCompiles|CheckCXXSourceCompiles|CheckCSymbolExists|CheckCXXSymbolExists' | grep -v Rust && echo "^ It's not allowed to have dynamic compiler checks with CMake."

PATTERN="allow_";
DIFF=$(comm -3 <(grep -o "\b$PATTERN\w*\b" $ROOT_PATH/src/Core/Settings.cpp | sort -u) <(grep -o -h "\b$PATTERN\w*\b" $ROOT_PATH/src/Databases/enableAllExperimentalSettings.cpp $ROOT_PATH/ci/jobs/scripts/check_style/experimental_settings_ignore.txt | sort -u));
[ -n "$DIFF" ] && echo "$DIFF" && echo "^^ Detected 'allow_*' settings that might need to be included in src/Databases/enableAllExperimentalSettings.cpp" && echo "Alternatively, consider adding an exception to ci/jobs/scripts/check_style/experimental_settings_ignore.txt"
} > "$O.11" 2>&1 &

# 12a: NDEBUG and cast checks on nobase_all
{
# A small typo can lead to debug code in release builds, see https://github.com/ClickHouse/ClickHouse/pull/47647
xargs < "$STYLE_TMPDIR/nobase_all" grep -l -F '#ifdef NDEBUG' | \
    xargs awk '/#ifdef NDEBUG/ { inside = 1; dirty = 1 } /#endif/ { if (inside && dirty) { print "File " FILENAME " has suspicious #ifdef NDEBUG, possibly confused with #ifndef NDEBUG" }; inside = 0 } /#else/ { dirty = 0 }'

# If a user is doing dynamic or typeid cast with a pointer, and immediately dereferencing it, it is unsafe.
xargs < "$STYLE_TMPDIR/nobase_all" rg --line-number '(dynamic|typeid)_cast<[^>]+\*>\([^\(\)]+\)->' | grep . && echo "It's suspicious when you are doing a dynamic_cast or typeid_cast with a pointer and immediately dereferencing it. Use references instead of pointers or check a pointer to nullptr."
} > "$O.12a" 2>&1 &

# 12b: Punctuation, std::regex, and Cyrillic checks on nobase_all
{
# Check for bad punctuation: whitespace before comma.
xargs < "$STYLE_TMPDIR/nobase_all" rg --line-number '\w ,' | grep -v 'bad punctuation is ok here' && echo "^ There is bad punctuation: whitespace before comma. You should write it like this: 'Hello, world!'"

# Check usage of std::regex which is too bloated and slow.
xargs < "$STYLE_TMPDIR/nobase_all" grep -F --line-number 'std::regex' | grep . && echo "^ Please use re2 instead of std::regex"

# Cyrillic characters hiding inside Latin.
grep -v StorageSystemContributors.generated.cpp "$STYLE_TMPDIR/nobase_all" | \
    xargs rg --line-number '[a-zA-Z][а-яА-ЯёЁ]|[а-яА-ЯёЁ][a-zA-Z]' && echo "^ Cyrillic characters found in unexpected place."
} > "$O.12b" 2>&1 &

# 13: Orphaned header files
{
join -v1 <(grep '\.h$' "$STYLE_TMPDIR/nobase_all" | sed 's:.*/::'  | sort -u) <(rg --no-filename -o '[\w-]+\.h' --glob '*.cpp' --glob '*.c' --glob '*.h' --glob '*.S' $ROOT_PATH/src $ROOT_PATH/programs $ROOT_PATH/utils $ROOT_PATH/tests/lexer | sort -u) |
    grep . && echo '^ Found orphan header files.'
} > "$O.13" 2>&1 &

# 14: Abbreviation checks and error message style
{
# Wrong spelling of abbreviations, e.g. SQL is right, Sql is wrong. XMLHttpRequest is very wrong.
xargs < "$STYLE_TMPDIR/all_excluded" rg 'Sql|Html|Xml|Cpu|Tcp|Udp|Http|Db|Json|Yaml' | grep -v -E 'RabbitMQ|Azure|Aws|aws|Avro|IO/S3|ai::JsonValue|IcebergWrites|arrow::flight|SqlInfo|CommandGetSqlInfo|CommandGetDbSchemas|commandGetDbSchemas|ArrowFlightSql|TcpExtListenOverflows' &&
    echo "Abbreviations such as SQL, XML, HTTP, should be in all caps. For example, SQL is right, Sql is wrong. XMLHttpRequest is very wrong."

xargs < "$STYLE_TMPDIR/all_excluded" grep -F -i 'ErrorCodes::LOGICAL_ERROR, "Logical error:' &&
    echo "If an exception has LOGICAL_ERROR code, there is no need to include the text 'Logical error' in the exception message, because then the phrase 'Logical error' will be printed twice."
} > "$O.14" 2>&1 &

# 15: magic_enum and std::format
{
# Don't allow the direct inclusion of magic_enum.hpp and instead point to base/EnumReflection.h
xargs < "$STYLE_TMPDIR/all_excluded" grep -l "magic_enum.hpp" | grep -v EnumReflection.h | while read -r line;
do
    echo "Found the inclusion of magic_enum.hpp in '${line}'. Please use <base/EnumReflection.h> instead"
done

# Currently fmt::format is faster both at compile and runtime
EXCLUDE_STD_FORMAT='HTTPHandler'
grep -vP $EXCLUDE_STD_FORMAT "$STYLE_TMPDIR/all_excluded" | xargs grep -l "std::format" | while read -r file;
do
    echo "Found the usage of std::format in '${file}'. Please use fmt::format instead"
done
} > "$O.15" 2>&1 &

# 16: Quote includes and shared_mutex
{
# Forbid using quotes for includes (except for autogenerated files)
grep -v 'utils/memcpy-bench/glibc/' "$STYLE_TMPDIR/nobase_excluded" | \
  xargs rg '#include\s*".*"' | \
  grep -v -F -e '"config.h"' -e '"config_tools.h"' -e '"SQLGrammar.pb.h"' -e '"out.pb.h"' -e '"clickhouse_grpc.grpc.pb.h"' -e '"delta_kernel_ffi.hpp"' | \
  sed "s/^/Found include with quotes in '/;s/$/'. Please use <> instead/"
} > "$O.16" 2>&1 &

# 17: Context.h usage
{
# Context.h (and a few similar headers) is included in many parts of the
# codebase, so any modifications to it trigger a large-scale recompilation.
# Therefore, it is crucial to avoid unnecessary inclusion of Context.h in
# headers.
#
# In most cases, we can include Context_fwd.h instead, as we usually do not
# need the full definition of the Context structure in headers - only declaration.
CONTEXT_H_EXCLUDES=(
    # For now we have few exceptions (somewhere due to templated code, in other
    # places just because for now it does not worth it, i.e. the header is not
    # too generic):
    --exclude "$ROOT_PATH/src/BridgeHelper/XDBCBridgeHelper.h"
    --exclude "$ROOT_PATH/src/Interpreters/AddDefaultDatabaseVisitor.h"
    --exclude "$ROOT_PATH/src/TableFunctions/ITableFunctionCluster.h"
    --exclude "$ROOT_PATH/src/Core/PostgreSQLProtocol.h"
    --exclude "$ROOT_PATH/src/Client/ClientBase.h"
    --exclude "$ROOT_PATH/src/Common/tests/gtest_global_context.h"
    --exclude "$ROOT_PATH/src/Analyzer/InDepthQueryTreeVisitor.h"
    --exclude "$ROOT_PATH/src/Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h"

    # For functions we allow it for regular functions (due to lots of
    # templates), but forbid it in interface (IFunction) part.
    --exclude "$ROOT_PATH/src/Functions/*"
    --include "$ROOT_PATH/src/Functions/IFunction*"
)
find $ROOT_PATH/src -name '*.h' -print0 | xargs -0 grep -P '#include[\s]*(<|")Interpreters/Context.h(>|")' "${CONTEXT_H_EXCLUDES[@]}" | \
    grep . && echo '^ Too broad Context.h usage. Consider using Context_fwd.h and Context.h out from .h into .cpp'
} > "$O.17" 2>&1 &

# Wait for all parallel checks to complete, then output results in order
wait
cat "$O".* 2>/dev/null

exit 0
