#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The built-in `/docs` page turns a relative documentation link into an in-app link when it points to
# another documented entity. Names are not unique across entity types: `JSON`, for example, is both a
# data type and a format. A link such as `[JSON](/interfaces/formats/JSON)` must open the `JSON`
# *format*, not the `JSON` *data type*. `rewriteLinks` therefore keeps all same-named entities and
# disambiguates by the link's target route (`typeFromDocsHref` / `resolveDocEntity`), leaving the link
# as an external docs link when the route gives no hint, instead of collapsing all types to one name.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs")"

# The page is served.
echo "$PAGE" | grep -oF 'ClickHouse <span class="accent">Reference</span>' | head -n1

# The renderer maps a docs route section to its entity type and resolves a link to the matching one.
echo "$PAGE" | grep -oF 'const DOCS_SECTION_TYPE = {' | head -n1
echo "$PAGE" | grep -oF "'formats': 'Format'," | head -n1
echo "$PAGE" | grep -oF 'function typeFromDocsHref(href)' | head -n1
echo "$PAGE" | grep -oF 'function resolveDocEntity(term, href)' | head -n1

# The regression target exists in the corpus: the name `JSON` is documented as BOTH a data type and a
# format (the ambiguous name that the route disambiguation must tell apart). Both are core, so they
# are present even in the minimal `Fast test` build (`ENABLE_LIBRARIES=0`).
$CLICKHOUSE_CLIENT --query "
    SELECT countIf(type = 'Data Type') > 0 AND countIf(type = 'Format') > 0
    FROM system.documentation
    WHERE name = 'JSON'"
