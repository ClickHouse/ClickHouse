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
    } || echo "Queries to system.query_log/system.query_thread_log does not have current_database = currentDatabase() condition in $test_case"
done

grep -iE 'SYSTEM STOP MERGES;?$' -R $ROOT_PATH/tests/queries && echo "Merges cannot be disabled globally in fast/stateful/stateless tests, because it will break concurrently running queries"


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
# NOTE: it is not that accuate, but at least something.
tests_with_database_column=( $(
    find $ROOT_PATH/tests/queries -iname '*.sql' -or -iname '*.sh' -or -iname '*.py' -or -iname '*.j2' |
        xargs grep --with-filename $(printf -- "-e %s " "${tables_with_database_column[@]}") |
        grep -v -e ':--' -e ':#' |
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
# NOTE: it is not that accuate, but at least something.
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

# The stateful directory should only contain the tests that depend on the test dataset (hits or visits).
find $ROOT_PATH/tests/queries/1_stateful -name '*.sql' -or -name '*.sh' | grep -v '00076_system_columns_bytes' | xargs -I{} bash -c 'grep -q -P "hits|visits" "{}" || echo "The test {} does not depend on the test dataset (hits or visits table) and should be located in the 0_stateless directory. You can also add an exception to the check-style script."'

# Check for existence of __init__.py files
for i in "${ROOT_PATH}"/tests/integration/test_*; do FILE="${i}/__init__.py"; [ ! -f "${FILE}" ] && echo "${FILE} should exist for every integration test"; done

# Check for executable bit on non-executable files
find $ROOT_PATH/{src,base,programs,utils,tests,docs,cmake} '(' -name '*.cpp' -or -name '*.h' -or -name '*.sql' -or -name '*.j2' -or -name '*.xml' -or -name '*.reference' -or -name '*.txt' -or -name '*.md' ')' -and -executable | grep -P '.' && echo "These files should not be executable."

# Check for BOM
find $ROOT_PATH/{src,base,programs,utils,tests,docs,cmake} -name '*.md' -or -name '*.cpp' -or -name '*.h' | xargs grep -l -F $'\xEF\xBB\xBF' | grep -P '.' && echo "Files should not have UTF-8 BOM"
find $ROOT_PATH/{src,base,programs,utils,tests,docs,cmake} -name '*.md' -or -name '*.cpp' -or -name '*.h' | xargs grep -l -F $'\xFF\xFE' | grep -P '.' && echo "Files should not have UTF-16LE BOM"
find $ROOT_PATH/{src,base,programs,utils,tests,docs,cmake} -name '*.md' -or -name '*.cpp' -or -name '*.h' | xargs grep -l -F $'\xFE\xFF' | grep -P '.' && echo "Files should not have UTF-16BE BOM"

# Conflict markers
find $ROOT_PATH/{src,base,programs,utils,tests,docs,cmake} -name '*.md' -or -name '*.cpp' -or -name '*.h' |
    xargs grep -P '^(<<<<<<<|=======|>>>>>>>)$' | grep -P '.' && echo "Conflict markers are found in files"

# DOS/Windows newlines
find $ROOT_PATH/{base,src,programs,utils,docs} -name '*.md' -or -name '*.h' -or -name '*.cpp' -or -name '*.js' -or -name '*.py' -or -name '*.html' | xargs grep -l -P '\r$' && echo "^ Files contain DOS/Windows newlines (\r\n instead of \n)."

# # workflows check
# act --list --directory="$ROOT_PATH" 1>/dev/null 2>&1 || act --list --directory="$ROOT_PATH" 2>&1
# actionlint -ignore 'reusable workflow call.+' || :
