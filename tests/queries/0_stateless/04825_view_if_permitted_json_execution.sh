#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `clickhouse_json` executes the deserialized AST directly, without formatting it back to SQL and
# reparsing, so `ASTFunction::readJSON` must canonicalize a non-canonical spelling of the
# `viewIfPermitted` table function name the same way the SQL parser does (`ViewLayer` dispatches on
# the lowercased name but always produces `viewIfPermitted`). Execution matches the name
# case-sensitively (e.g. `StorageView::replaceWithSubquery`), so without the canonicalization a
# payload with `"VIEWIFPERMITTED"` and the valid (select, function) shape passed deserialization but
# failed at execution with a logical error ("Incorrect table expression").

JSON=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary @- \
    <<< "SELECT parseQueryToJSON('SELECT * FROM viewIfPermitted(SELECT 42 AS x ELSE null(''x UInt8''))') FORMAT TSVRaw")

# The canonical spelling executes.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_json_ast_dialect=1&dialect=clickhouse_json" --data-binary "$JSON"

# A non-canonical spelling of the table function name executes the same way.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_json_ast_dialect=1&dialect=clickhouse_json" \
    --data-binary "${JSON//\"name\":\"viewIfPermitted\"/\"name\":\"VIEWIFPERMITTED\"}"
