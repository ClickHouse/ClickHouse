#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: requires the ORC input format, which is not built in fasttest.

# Regression test for a schema-inference cache-key bug in the native ORC reader.
# `max_parser_depth` changes the outcome of schema inference: a deeply nested ORC schema is
# rejected with `TOO_DEEP_RECURSION` when the nesting exceeds the limit. The setting must therefore
# be part of the schema-inference cache key. Otherwise a query that caches the deep schema under a
# permissive limit poisons the cache for a later query with a lower limit, which would then reuse
# the cached schema instead of throwing.
#
# The data file has a single column `a` of type `list<list<list<long>>>` (written by ClickHouse
# itself). Both DESC queries run in a single clickhouse-local process so they share the
# schema-inference cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=${CLICKHOUSE_TMP}/04631_nested_arrays.orc

# Write the file in a separate process: a freshly written file can be seen as modified by the
# schema cache of the same process, which would bypass the cache and hide the bug.
$CLICKHOUSE_LOCAL -q "INSERT INTO FUNCTION file('$DATA_FILE', ORC) SELECT [[[1::Int64]]] AS a SETTINGS engine_file_truncate_on_insert = 1"

$CLICKHOUSE_LOCAL --multiquery "
    -- Infer and cache the deeply nested schema under the default (permissive) limit.
    DESC file('$DATA_FILE', ORC);
    -- Infer again with a lower limit: must NOT reuse the cached schema and must throw.
    DESC file('$DATA_FILE', ORC) SETTINGS max_parser_depth = 1;
" 2>&1 | grep -oE 'Array\(Array\(Array\(Nullable\(Int64\)\)\)\)|TOO_DEEP_RECURSION' | head -2

rm -f "$DATA_FILE"
