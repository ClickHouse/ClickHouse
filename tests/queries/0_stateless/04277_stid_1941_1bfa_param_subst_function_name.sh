#!/usr/bin/env bash

# Regression test for STID 1941-1bfa:
#   Logical error: 'Inconsistent AST formatting: the original AST: (STID: 1941-1bfa)'
#
# A query parameter substitution in a function-name position (single-part like
# `{F:Identifier}(args)` or compound like `{db:Identifier}.tbl.fn(args)`) used to
# build an ASTFunction with an empty `name`, because `ASTIdentifier::name()`
# returns the unset `full_name` when `name_parts` contains parameter holes.
# `ASTFunction::formatImplWithoutAlias` then emitted just `(args)` (skipping the
# missing name), and the re-parser saw a plain parenthesized expression, so the
# round-trip AST tree hash differed from the original and `executeQueryImpl`
# raised the `Inconsistent AST formatting` exception that aborts the server in
# debug builds. This is the chronic trunk failure the AST fuzzer rediscovers
# across PRs; see issue #104605.
#
# The fix rejects such an identifier as a function name with a `SYNTAX_ERROR`
# at parse time in `getFunctionLayer`.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# --- Reproducers that previously triggered the exception ---------------------

# Minimal: single-part parameter as the whole function name.
${CLICKHOUSE_CLIENT} --param_FUNC='abs' \
    --query "SELECT {FUNC:Identifier}(-1)" 2>&1 | grep -o 'SYNTAX_ERROR'

# Compound parameter prefix with the COLUMNS matcher fallback path.
${CLICKHOUSE_CLIENT} --param_DB="${CLICKHOUSE_DATABASE}" \
    --query "DROP TABLE IF EXISTS test_table" \
    && ${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_table (id Int) ENGINE = Memory" \
    && ${CLICKHOUSE_CLIENT} --query "INSERT INTO test_table VALUES (1), (2), (3)"

${CLICKHOUSE_CLIENT} --param_DB="${CLICKHOUSE_DATABASE}" \
    --query "SELECT {DB:Identifier}.test_table.COLUMNS(if(1, 1, id)) FROM {DB:Identifier}.test_table" 2>&1 | grep -o 'SYNTAX_ERROR'

# `DESCRIBE TABLE` wrapping the same shape (the AST-fuzzer-found trigger).
${CLICKHOUSE_CLIENT} --param_DB="${CLICKHOUSE_DATABASE}" \
    --query "DESCRIBE TABLE (SELECT {DB:Identifier}.test_table.COLUMNS(if(1, 1, id)) FROM {DB:Identifier}.test_table)" 2>&1 | grep -o 'SYNTAX_ERROR'

# --- Sanity checks: legitimate parameter usages still work -------------------

# Parameter as column name.
${CLICKHOUSE_CLIENT} --param_COL='id' \
    --query "SELECT {COL:Identifier} FROM test_table ORDER BY id"

# Parameter as table name.
${CLICKHOUSE_CLIENT} --param_TBL='test_table' \
    --query "SELECT count() FROM {TBL:Identifier}"

# Parameter as database name in qualified column reference.
${CLICKHOUSE_CLIENT} --param_DB="${CLICKHOUSE_DATABASE}" \
    --query "SELECT count() FROM {DB:Identifier}.test_table"

# `COLUMNS` matcher (no parameter in function-name position) still works.
${CLICKHOUSE_CLIENT} --query "SELECT COLUMNS('id') FROM test_table ORDER BY id"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_table"
