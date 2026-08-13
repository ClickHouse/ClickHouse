#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The archive-vs-plain split of `allow_archive_path_syntax` must honor the selected glob parser.
# To the AST parser a literal brace group like `{x}` is constant text, so `data_{x}::foo.csv`
# is an exact filename; only the legacy parser treats `data_{x}` as a glob and therefore as a
# possible archive path. Before the fix the split raw-scanned for `*?{` regardless of
# `use_glob_ast_parser`, so the AST session tried to open `data_{x}` as an archive.
# https://github.com/ClickHouse/ClickHouse/pull/91062

db=${CLICKHOUSE_DATABASE}

# Create a file whose name literally contains a brace group and the archive separator.
${CLICKHOUSE_CLIENT} --use_glob_ast_parser=1 --allow_archive_path_syntax=0 --query "INSERT INTO FUNCTION file('${db}/archive_split/data_{x}::foo.csv', CSV, 'c1 Int32') SELECT 42"

echo "AST parser reads the exact filename:"
${CLICKHOUSE_CLIENT} --use_glob_ast_parser=1 --allow_archive_path_syntax=1 --query "SELECT * FROM file('${db}/archive_split/data_{x}::foo.csv', CSV, 'c1 Int32')"

echo "Legacy parser keeps archive syntax (glob archive path matches nothing):"
${CLICKHOUSE_CLIENT} --use_glob_ast_parser=0 --allow_archive_path_syntax=1 --query "SELECT * FROM file('${db}/archive_split/data_{x}::foo.csv', CSV, 'c1 Int32')"

echo "AST parser writes the exact filename under archive syntax:"
${CLICKHOUSE_CLIENT} --use_glob_ast_parser=1 --allow_archive_path_syntax=1 --query "INSERT INTO FUNCTION file('${db}/archive_split/data_{y}::bar.csv', CSV, 'c1 Int32') SELECT 43"
${CLICKHOUSE_CLIENT} --use_glob_ast_parser=1 --allow_archive_path_syntax=1 --query "SELECT * FROM file('${db}/archive_split/data_{y}::bar.csv', CSV, 'c1 Int32')"

echo "AST parser still takes archive syntax for a real enum glob (matches nothing):"
${CLICKHOUSE_CLIENT} --use_glob_ast_parser=1 --allow_archive_path_syntax=1 --query "SELECT * FROM file('${db}/archive_split/data_{a,b}::foo.csv', CSV, 'c1 Int32')"
