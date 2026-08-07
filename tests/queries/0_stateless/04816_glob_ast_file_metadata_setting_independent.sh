#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The meaning of persisted `ENGINE = File` metadata must not depend on the per-session
# `use_glob_ast_parser` setting: to the AST parser a literal brace group like `{x}` is constant
# text (not a glob), but the paths resolved at CREATE/ATTACH become table state, so letting the
# session setting through would make the same stored DDL resolve to a different file across
# sessions and server restarts. The DDL path always uses the legacy classification.
# https://github.com/ClickHouse/ClickHouse/pull/91062

db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${db}/metadata_guard/data_x.csv', CSV, 'c1 Int32') SELECT 42"

# Under the AST parser `data_{x}.csv` would be an exact (non-glob) path; the legacy parser treats
# it as a glob matching `data_x.csv`. The table must resolve to the existing file either way.
${CLICKHOUSE_CLIENT} --use_glob_ast_parser=1 --query "CREATE TABLE t_glob_ast_file (c1 Int32) ENGINE = File(CSV, '${db}/metadata_guard/data_{x}.csv')"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_glob_ast_file"

# Re-attaching under either setting must not reinterpret the stored path.
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_glob_ast_file"
${CLICKHOUSE_CLIENT} --use_glob_ast_parser=0 --query "ATTACH TABLE t_glob_ast_file"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_glob_ast_file"

${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_glob_ast_file"
${CLICKHOUSE_CLIENT} --use_glob_ast_parser=1 --query "ATTACH TABLE t_glob_ast_file"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_glob_ast_file"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_glob_ast_file"
