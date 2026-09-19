#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# In the `clickhouse_json` dialect, `clickhouse-local`'s `input()` initializer reparses the original
# query text under the parser/AST limits captured when the query was received - before the query's own
# `SETTINGS` clause (already folded into the sent settings by the client) is overlaid. A query-local
# limit must therefore behave exactly as in the SQL dialect: a limit the query itself does not fit is
# rejected up front by the main parse, and a satisfiable one must not make the `input()` reparse fail.

# 1. A query-local `max_ast_depth` too small for the query itself: rejected up front with `TOO_DEEP_AST`,
#    identically in the SQL and JSON dialects (never a late failure inside the `input()` initializer).
SQL_QUERY="INSERT INTO FUNCTION null('x UInt8') SELECT * FROM input('x UInt8') SETTINGS max_ast_depth = 1 FORMAT TSV"
printf '1\n2\n3\n' | ${CLICKHOUSE_LOCAL} -q "$SQL_QUERY" 2>&1 | grep -o 'TOO_DEEP_AST'
JSON_QUERY=$(${CLICKHOUSE_LOCAL} -q "SELECT parseQueryToJSON('INSERT INTO FUNCTION null(''x UInt8'') SELECT * FROM input(''x UInt8'') SETTINGS max_ast_depth = 1 FORMAT TSV') FORMAT TSVRaw")
printf '1\n2\n3\n' | ${CLICKHOUSE_LOCAL} --enable_json_ast_dialect 1 --dialect clickhouse_json -q "$JSON_QUERY" 2>&1 | grep -o 'TOO_DEEP_AST'

# 2. A satisfiable query-local `max_ast_depth` with a tight session limit: the query is accepted and the
#    `input()` reparse succeeds under the limits the text was originally accepted with.
JSON_OK=$(${CLICKHOUSE_LOCAL} -q "SELECT parseQueryToJSON('INSERT INTO FUNCTION null(''x UInt8'') SELECT * FROM input(''x UInt8'') SETTINGS max_ast_depth = 32 FORMAT TSV') FORMAT TSVRaw")
printf '1\n2\n3\n' | ${CLICKHOUSE_LOCAL} --max_ast_depth 25 --enable_json_ast_dialect 1 --dialect clickhouse_json -q "$JSON_OK" 2>&1 && echo 'json_query_local_limit_ok'

# 3. A query-local `max_parser_depth` the query itself does not fit is likewise rejected up front,
#    identically in both dialects.
printf '1\n2\n3\n' | ${CLICKHOUSE_LOCAL} -q "INSERT INTO FUNCTION null('x UInt8') SELECT * FROM input('x UInt8') SETTINGS max_parser_depth = 1 FORMAT TSV" 2>&1 | grep -o 'TOO_DEEP_RECURSION'
JSON_PARSER=$(${CLICKHOUSE_LOCAL} -q "SELECT parseQueryToJSON('INSERT INTO FUNCTION null(''x UInt8'') SELECT * FROM input(''x UInt8'') SETTINGS max_parser_depth = 1 FORMAT TSV') FORMAT TSVRaw")
printf '1\n2\n3\n' | ${CLICKHOUSE_LOCAL} --enable_json_ast_dialect 1 --dialect clickhouse_json -q "$JSON_PARSER" 2>&1 | grep -o 'TOO_DEEP_RECURSION'
