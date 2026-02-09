#!/bin/bash

ROOT_PATH="."

# Queries to system.query_log/system.query_thread_log should have current_database = currentDatabase() condition
# NOTE: it is not that accurate, but at least something.
tests_with_query_log=( $(
    find $ROOT_PATH/tests/queries -iname '*.sql' -or -iname '*.sh' -or -iname '*.py' -or -iname '*.j2' |
        xargs grep --with-filename -e system.query_log -e system.query_thread_log | cut -d: -f1 | sort -u
) )
for test_case in "${tests_with_query_log[@]}"; do
    grep -qE current_database.*currentDatabase "$test_case" || {
        grep -qE 'current_database.*\$CLICKHOUSE_DATABASE' "$test_case"
    } || {
        grep -qE 'has\(databases\,\ currentDatabase\(\)\)' "$test_case"
    } || {
        grep -qE 'has\(databases\,\ current_database\(\)\)' "$test_case"
    } || echo "Query to system.query_log/system.query_thread_log does not have current_database = currentDatabase() condition in $test_case"
done

grep -iE 'SYSTEM STOP MERGES;?$' -R $ROOT_PATH/tests/queries && echo "Merges cannot be disabled globally in fast/stateless tests, because it will break concurrently running queries"


# Queries to:
tables_with_database_column=(
    system.tables
    system.parts
    system.detached_parts
    system.parts_columns
    system.columns
    system.projection_parts
    system.mutations
)
# should have database = currentDatabase() condition
#
# NOTE: it is not that accurate, but at least something.
tests_with_database_column=( $(
    find $ROOT_PATH/tests/queries -iname '*.sql' -or -iname '*.sh' -or -iname '*.py' -or -iname '*.j2' |
        xargs grep --with-filename $(printf -- "-e %s " "${tables_with_database_column[@]}") |
        grep -v -e ':--' -e ':#' |
        # to exclude clickhouse-local flags: --only-system-tables and --no-system-tables.
        grep -v -e '--[a-zA-Z-]*system[a-zA-Z-]*' |
        cut -d: -f1 | sort -u
) )
for test_case in "${tests_with_database_column[@]}"; do
    grep -qE database.*currentDatabase "$test_case" || {
        grep -qE 'database.*\$CLICKHOUSE_DATABASE' "$test_case"
    } || {
        # explicit database
        grep -qE "database[ ]*=[ ]*'" "$test_case"
    } || {
        echo "Queries to ${tables_with_database_column[*]} does not have database = currentDatabase()/\$CLICKHOUSE_DATABASE condition in $test_case"
    }
done

# Queries with ReplicatedMergeTree
# NOTE: it is not that accurate, but at least something.
tests_with_replicated_merge_tree=( $(
    find $ROOT_PATH/tests/queries -iname '*.sql' -or -iname '*.sh' -or -iname '*.py' -or -iname '*.j2' |
        xargs grep --with-filename -e "Replicated.*MergeTree[ ]*(.*" | cut -d: -f1 | sort -u
) )
for test_case in "${tests_with_replicated_merge_tree[@]}"; do
    case "$test_case" in
        *.gen.*)
            ;;
        *.sh)
            test_case_zk_prefix="\(\$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX\|{database}\)"
            grep -q -e "Replicated.*MergeTree[ ]*(.*$test_case_zk_prefix" "$test_case" || echo "Replicated.*MergeTree should contain '$test_case_zk_prefix' in zookeeper path to avoid overlaps ($test_case)"
            ;;
        *.sql|*.sql.j2)
            test_case_zk_prefix="\({database}\|currentDatabase()\|{uuid}\|{default_path_test}\)"
            grep -q -e "Replicated.*MergeTree[ ]*(.*$test_case_zk_prefix" "$test_case" || echo "Replicated.*MergeTree should contain '$test_case_zk_prefix' in zookeeper path to avoid overlaps ($test_case)"
            ;;
        *.py)
            # Right now there is not such tests anyway
            echo "No ReplicatedMergeTree style check for *.py ($test_case)"
            ;;
    esac
done

# Check for existence of __init__.py files
# This check is necessary to prevent issues with Python imports when test directories are missing this file.
# See https://stackoverflow.com/questions/53918088/import-file-mismatch-in-pytest
for i in "${ROOT_PATH}"/tests/integration/test_*; do FILE="${i}/__init__.py"; [ ! -f "${FILE}" ] && echo "${FILE} should exist for every integration test"; done

# Check for executable bit on non-executable files
git ls-files -s $ROOT_PATH/{src,base,programs,utils,tests,docs,cmake} | \
    awk '$1 != "120000" && $1 != "100644" { print $4 }' | grep -E '\.(cpp|h|sql|j2|xml|reference|txt|md)$' && echo "These files should not be executable."

# Check for BOM
find $ROOT_PATH/{src,base,programs,utils,tests,docs,cmake} -name '*.md' -or -name '*.cpp' -or -name '*.h' | xargs grep -l -F $'\xEF\xBB\xBF' | grep -P '.' && echo "Files should not have UTF-8 BOM"
find $ROOT_PATH/{src,base,programs,utils,tests,docs,cmake} -name '*.md' -or -name '*.cpp' -or -name '*.h' | xargs grep -l -F $'\xFF\xFE' | grep -P '.' && echo "Files should not have UTF-16LE BOM"
find $ROOT_PATH/{src,base,programs,utils,tests,docs,cmake} -name '*.md' -or -name '*.cpp' -or -name '*.h' | xargs grep -l -F $'\xFE\xFF' | grep -P '.' && echo "Files should not have UTF-16BE BOM"

# Ensure that functions do not hold copy of ContextPtr
#
# ContextPtr holds lots of different stuff, including some caches, so prefer to
# use weak_ptr or copy relevant info from Context, to avoid extending lifetime
# of other objects.
#
# NOTE: most of the excludes here needs context to call another function, which
# is easy to fix.
FUNCTIONS_CONTEXT_PTR_EXCEPTIONS=(
    -e /trap.cpp
    -e /toInterval.cpp
    -e /structureToFormatSchema.cpp
    -e /splitByRegexp.cpp
    -e /reverse.cpp
    -e /nullIf.cpp
    -e /midpoint.h
    -e /ifNull.cpp
    -e /ifNotFinite.cpp
    -e /generateSerialID.cpp
    -e /formatRow.cpp
    -e /filesystem.cpp
    -e /evalMLMethod.cpp
    -e /date_trunc.cpp
    -e /currentRoles.cpp
    -e /currentProfiles.cpp
    -e /concat.cpp
    -e /caseWithExpression.cpp
    -e /CastOverloadResolver.cpp
    -e /array/arrayRemove.h
    -e /array/arrayJaccardIndex.cpp
    -e /array/arrayIntersect.cpp
    -e /array/arrayElement.cpp
    -e /UserDefined/
    -e /LeastGreatestGeneric.h
    -e /Kusto/KqlArraySort.cpp
    -e /FunctionsOpDate.cpp
    -e /FunctionUnaryArithmetic.h
    -e /FunctionNaiveBayesClassifier.cpp
    -e /FunctionBinaryArithmetic.h
    -e /ITupleFunction.h

    -e /FunctionJoinGet.cpp
    -e /FunctionsExternalDictionaries.cpp
    -e /FunctionsExternalDictionaries.h
    -e /FunctionDictGetKeys.cpp

    -e /TimeSeries/timeSeriesIdToTagsGroup.cpp
    -e /TimeSeries/timeSeriesIdToTags.cpp
    -e /TimeSeries/timeSeriesTagsGroupToTags.cpp
    -e /TimeSeries/timeSeriesStoreTags.cpp
)
find $ROOT_PATH/src/Functions -type f | xargs grep -l 'ContextPtr [a-z_]*;' | grep -v "${FUNCTIONS_CONTEXT_PTR_EXCEPTIONS[@]}" | grep -P '.' && echo "Avoid holding a copy of ContextPtr in Functions"

# Ensure that functions do not use WithContext, since this may lead to expired context (when it is used in MergeTree).
FUNCTIONS_WITH_CONTEXT_EXCEPTIONS=(
    # It is OK to have WithContext for derived classes from IFunctionOverloadResolver
    -e /FunctionJoinGet.cpp
    # Store global context
    -e /ExternalUserDefinedExecutableFunctionsLoader.cpp
    # Used only in getReturnTypeImpl()
    -e /array/arrayReduce.cpp
    -e /array/arrayReduceInRanges.cpp
    # Global context
    -e /catboostEvaluate.cpp
    # Always constant
    -e /connectionId.cpp
    # Do not leak HTTP headers to MergeTree
    -e /getClientHTTPHeader.cpp
    # Avoid leaking
    -e /getMergeTreeSetting.cpp
    -e /getScalar.cpp
    -e /getSetting.cpp
    -e /hasColumnInTable.cpp
    -e /initializeAggregation.cpp
)
find $ROOT_PATH/src/Functions -type f | xargs grep -l 'WithContext(' | grep -v "${FUNCTIONS_WITH_CONTEXT_EXCEPTIONS[@]}" | grep -P '.' && echo "Avoid using WithContext in Functions"

# Conflict markers
find $ROOT_PATH/{src,base,programs,utils,tests,docs,cmake} -name '*.md' -or -name '*.cpp' -or -name '*.h' |
    xargs grep -P '^(<<<<<<<|=======|>>>>>>>)$' | grep -P '.' && echo "Conflict markers are found in files"

# DOS/Windows newlines
find $ROOT_PATH/{base,src,programs,utils,docs} -name '*.md' -or -name '*.h' -or -name '*.cpp' -or -name '*.js' -or -name '*.py' -or -name '*.html' | xargs grep -l -P '\r$' && echo "^ Files contain DOS/Windows newlines (\r\n instead of \n)."

# Check for misuse of timeout in .sh tests
find $ROOT_PATH/tests/queries -name '*.sh' |
    grep -vP '02835_drop_user_during_session|02922_deduplication_with_zero_copy|00738_lock_for_inner_table|shared_merge_tree|_sc_|03710_parallel_alter_comment_rename_selects' |
    xargs grep -l -P 'export -f' |
    xargs grep -l -F 'timeout' &&
    echo ".sh tests cannot use the 'timeout' command, because it leads to race conditions, when the timeout is expired, and waiting for the command is done, but the server still runs some queries"

find $ROOT_PATH/tests/queries -iname '*.sql' -or -iname '*.sh' -or -iname '*.py' -or -iname '*.j2' | xargs grep --with-filename -i -E -e 'system\s*flush\s*logs\s*(;|$|")' && echo "Please use SYSTEM FLUSH LOGS log_name over global SYSTEM FLUSH LOGS"

# CLICKHOUSE_URL already includes "?"
git grep -P 'CLICKHOUSE_URL(|_HTTPS)(}|}/|/|)\?' $ROOT_PATH/tests/queries/0_stateless/*.sh
