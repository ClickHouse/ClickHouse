#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When the AST glob parser's enum expansion exceeds `glob_expansion_max_elements`, the listing
# traverses the unexpanded pattern once, which collapses overlapping enum alternatives:
# `{top,top}.csv` then matches `top.csv` once instead of twice. The glob classification must not
# be derived from the collapsed path count, otherwise lowering the cap would turn a globbed
# (readonly) destination into an exact writable path.
# https://github.com/ClickHouse/ClickHouse/pull/91062

db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${db}/expansion_cap/top.csv', CSV, 'c1 Int32') SELECT 1"

# Fully expanded (cap not hit): two overlapping alternatives match the same file, one row each.
${CLICKHOUSE_CLIENT} --use_glob_ast_parser=1 --query "SELECT count() FROM file('${db}/expansion_cap/{top,top}.csv', CSV, 'c1 Int32')"

# Over the cap: the unexpanded traversal matches the file once, but the pattern is still a glob.
${CLICKHOUSE_CLIENT} --use_glob_ast_parser=1 --glob_expansion_max_elements=1 --query "SELECT count() FROM file('${db}/expansion_cap/{top,top}.csv', CSV, 'c1 Int32')"

# A globbed destination must stay readonly on the over-cap path.
${CLICKHOUSE_CLIENT} --use_glob_ast_parser=1 --glob_expansion_max_elements=1 --query "INSERT INTO FUNCTION file('${db}/expansion_cap/{top,top}.csv', CSV, 'c1 Int32') SELECT 2" 2>&1 | grep -o -m1 'DATABASE_ACCESS_DENIED'

# The file must be untouched by the rejected insert.
${CLICKHOUSE_CLIENT} --query "SELECT * FROM file('${db}/expansion_cap/top.csv', CSV, 'c1 Int32')"
