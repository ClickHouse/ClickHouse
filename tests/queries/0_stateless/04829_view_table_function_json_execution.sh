#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `clickhouse_json` executes the deserialized AST directly, without formatting it back to SQL and
# reparsing, so `ASTFunction::readJSON` must canonicalize a non-canonical spelling of the `view`
# table function name the same way the SQL parser does (`ViewLayer` dispatches on the lowercased
# name but always produces `view`). Execution matches the name case-sensitively (e.g.
# `StorageView::replaceWithSubquery` and the table function factory), so without the
# canonicalization a payload with `"VIEW"` and the valid single-bare-select shape passed
# deserialization but failed at execution.

JSON=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @- \
    <<< "SELECT parseQueryToJSON('SELECT * FROM view(SELECT 42 AS x)') FORMAT TSVRaw")

# The canonical spelling executes.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_json_ast_dialect=1&dialect=clickhouse_json" --data-binary "$JSON"

# A non-canonical spelling of the table function name executes the same way.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_json_ast_dialect=1&dialect=clickhouse_json" \
    --data-binary "${JSON//\"name\":\"view\"/\"name\":\"VIEW\"}"
