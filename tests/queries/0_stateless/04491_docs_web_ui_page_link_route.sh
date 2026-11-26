#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The built-in `/docs` page retains `DOCS_PAGE_ROUTE` for legacy relative links to standalone pages
# whose route cannot be inferred from a documented entity. Current embedded documentation uses
# site-root absolute routes instead, so it preserves the intended page and anchor without depending on
# this compatibility mapping.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs")"

# The page is served.
echo "$PAGE" | grep -oF 'ClickHouse <span class="accent">Reference</span>' | head -n1

# `toDocsURL` maps a standalone docs page to its canonical route via `DOCS_PAGE_ROUTE`.
echo "$PAGE" | grep -oF 'const DOCS_PAGE_ROUTE = {' | head -n1
echo "$PAGE" | grep -oF "'syntax': '/sql-reference/syntax'," | head -n1
echo "$PAGE" | grep -oF "'type-conversion-functions': '/sql-reference/functions/type-conversion-functions'," | head -n1
echo "$PAGE" | grep -oF 'if (DOCS_PAGE_ROUTE[head]) return base + DOCS_PAGE_ROUTE[head] + tail + suffix;' | head -n1
# The page-load compatibility assertion feeds both original standalone-page links to `toDocsURL`.
echo "$PAGE" | grep -oF "'type-conversion-functions.md#parseDateTime64BestEffort': 'https://clickhouse.com/docs/sql-reference/functions/type-conversion-functions#parseDateTime64BestEffort'," | head -n1
echo "$PAGE" | grep -oF "'../syntax.md#string': 'https://clickhouse.com/docs/sql-reference/syntax#string'," | head -n1

# `toWeek` and `like` are core functions, present even in the minimal `Fast test` build
# (`ENABLE_LIBRARIES=0`). Their embedded documentation uses current site-root absolute routes and no
# longer contains the legacy relative forms.
$CLICKHOUSE_CLIENT --query "
    SELECT
        position(description, '/reference/functions/regular-functions/type-conversion-functions#parseDateTime64BestEffort') > 0
            AND position(description, 'type-conversion-functions.md#parseDateTime64BestEffort') = 0
    FROM system.documentation
    WHERE type = 'Function' AND name = 'toWeek'"
$CLICKHOUSE_CLIENT --query "
    SELECT
        position(description, '/reference/syntax#string') > 0
            AND position(description, '../syntax.md#string') = 0
    FROM system.documentation
    WHERE type = 'Function' AND name = 'like'"
