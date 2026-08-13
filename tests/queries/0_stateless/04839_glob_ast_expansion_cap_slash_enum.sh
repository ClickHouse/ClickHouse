#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The over-cap fallback of the AST glob parser traverses the unexpanded pattern, splitting it at
# every raw '/'. An enum whose alternatives span path segments (`{a/b,c/d}.csv`) is cut apart by
# that split and would match nothing, so lowering `glob_expansion_max_elements` must fail closed
# instead of silently returning an empty result.
# https://github.com/ClickHouse/ClickHouse/pull/91062

db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${db}/slash_enum/a/b.csv', CSV, 'c1 Int32') SELECT 1"
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${db}/slash_enum/c/d.csv', CSV, 'c1 Int32') SELECT 2"

# Below the cap the enum is expanded and both files are read.
${CLICKHOUSE_CLIENT} --use_glob_ast_parser=1 --query "SELECT sum(c1) FROM file('${db}/slash_enum/{a/b,c/d}.csv', CSV, 'c1 Int32')"

# Above the cap the pattern cannot be matched unexpanded, so it must be rejected, not silently empty.
${CLICKHOUSE_CLIENT} --use_glob_ast_parser=1 --glob_expansion_max_elements=1 --query "SELECT sum(c1) FROM file('${db}/slash_enum/{a/b,c/d}.csv', CSV, 'c1 Int32')" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

# An enum that stays inside a single path segment keeps using the unexpanded traversal.
${CLICKHOUSE_CLIENT} --use_glob_ast_parser=1 --glob_expansion_max_elements=1 --query "SELECT sum(c1) FROM file('${db}/slash_enum/a/{b,b}.csv', CSV, 'c1 Int32')"

# The legacy parser expands unconditionally, so the cap does not affect it.
${CLICKHOUSE_CLIENT} --use_glob_ast_parser=0 --glob_expansion_max_elements=1 --query "SELECT sum(c1) FROM file('${db}/slash_enum/{a/b,c/d}.csv', CSV, 'c1 Int32')"
