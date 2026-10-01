#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `FORMAT` takes a plain identifier in SQL, so AST JSON must not build it from a query parameter:
# the formatted query could not be parsed back, and `clickhouse_json` would run a different query.
select_with_format()
{
    echo '{"type":"SelectWithUnionQuery","union_mode":"UNION_DEFAULT","list_of_selects":{"type":"ExpressionList","children":[{"type":"SelectQuery","select":{"type":"ExpressionList","children":[{"type":"Literal","value":{"field_type":"UInt64","value":1}}]}}]},"format_ast":'"$1"',"is_outfile_append":false,"is_outfile_truncate":false,"is_into_outfile_with_stdout":false}'
}

plain_format='{"type":"Identifier","name":"TSV"}'
parameter_format='{"type":"Identifier","name":"","children":[{"type":"QueryParameter","name":"fmt","param_type":"Identifier"}]}'

for format in "$plain_format" "$parameter_format"
do
    $CLICKHOUSE_CLIENT --query "SELECT formatQueryFromJSON('$(select_with_format "$format")')" 2>&1 | grep -o 'SELECT 1 FORMAT TSV\|BAD_ARGUMENTS' | head -1
    $CLICKHOUSE_CLIENT --enable_json_ast_dialect=1 --dialect=clickhouse_json --param_fmt=TSV --query "$(select_with_format "$format")" 2>&1 | grep -o '^1$\|BAD_ARGUMENTS' | head -1
done
