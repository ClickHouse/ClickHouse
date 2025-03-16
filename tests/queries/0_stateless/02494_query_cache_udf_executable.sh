#!/usr/bin/env bash
# Tags: no-parallel
# Tag no-parallel: Messes with internal cache

# Test for issue #77553: External UDFs may be non-deterministic. The query cache should treat them as such, i.e. reject them.
# Also see 02494_query_cache_udf_sql.sql

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SCRIPTS_DIR=$CUR_DIR/scripts_udf

$CLICKHOUSE_LOCAL -q "
SYSTEM DROP QUERY CACHE;
" -q "
SELECT '-- query_cache_nondeterministic_function_handling = throw';
SELECT test_function() SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'throw'; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT count(*) FROM system.query_cache;
SYSTEM DROP QUERY CACHE;
" -q "
SELECT '-- query_cache_nondeterministic_function_handling = save';
SELECT test_function() SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'save';
SELECT count(*) FROM system.query_cache;
SYSTEM DROP QUERY CACHE;
" -q "
SELECT '-- query_cache_nondeterministic_function_handling = ignore';
SELECT test_function() SETTINGS use_query_cache = true, query_cache_nondeterministic_function_handling = 'ignore';
SELECT count(*) FROM system.query_cache;
SYSTEM DROP QUERY CACHE;
" -- --user_scripts_path=$SCRIPTS_DIR \
                  --user_defined_executable_functions_config=$SCRIPTS_DIR/function.xml \
                  --input_format_tsv_detect_header=1
