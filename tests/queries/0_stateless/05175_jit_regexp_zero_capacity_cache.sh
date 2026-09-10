#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: the Fast test build has no embedded compiler, so no regular expression is ever compiled.
# no-msan: the MSan build disables the embedded compiler too (contrib/llvm-project-cmake/CMakeLists.txt).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A compiled matcher is only worth its LLVM compile if the compiled-expression cache hands it to the next
# call, and a cache configured with zero capacity evicts every entry on insert. `max_block_size = 1` makes
# every row its own call, so the compile count separates reuse from a recompile per call.
query="SELECT count() FROM numbers(4) WHERE match(materialize(toString(number)), '^id[0-9]+/.*\$')
    SETTINGS compile_regular_expressions = 1, min_count_to_compile_regular_expression = 0,
             max_block_size = 1, max_threads = 1 FORMAT Null;
SELECT sum(value) FROM system.events WHERE event = 'CompileRegexpFunction'"

echo '-- zero-capacity compiled-expression cache: nothing is retained, so nothing is compiled'
${CLICKHOUSE_LOCAL} --query "$query" -- --compiled_expression_cache_size=0
echo '-- default compiled-expression cache: compiled once, then reused by the other three calls'
${CLICKHOUSE_LOCAL} --query "$query"
