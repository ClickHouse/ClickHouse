#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Some embedded documentation links point to a standalone docs page that is neither a documented entity nor a
# section directory, with a source-relative path: `toWeek`/`toYearWeek` link to
# `type-conversion-functions.md#parseDateTime64BestEffort` and `like` links to `../syntax.md#string`.
# `candidateTerm` cannot map such a link to an entity (the link text is not a bare identifier and the path's
# last segment is not a documented name), and the page's docs section is not derivable from the relative path
# (the entity's embedded `source` is a code path, not a docs path). `toDocsURL` therefore used to fall back to
# the bare `https://clickhouse.com/docs` root, losing the target page and its `#anchor`. `DOCS_PAGE_ROUTE` maps
# each such page to its canonical docs route so the link resolves to the intended page instead.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs")"

# The page is served.
echo "$PAGE" | grep -oF 'ClickHouse <span class="accent">Reference</span>' | head -n1

# `toDocsURL` maps a standalone docs page to its canonical route via `DOCS_PAGE_ROUTE`.
echo "$PAGE" | grep -oF 'const DOCS_PAGE_ROUTE = {' | head -n1
echo "$PAGE" | grep -oF "'syntax': '/sql-reference/syntax'," | head -n1
echo "$PAGE" | grep -oF "'type-conversion-functions': '/sql-reference/functions/type-conversion-functions'," | head -n1
echo "$PAGE" | grep -oF 'if (DOCS_PAGE_ROUTE[head]) return base + DOCS_PAGE_ROUTE[head] + tail + suffix;' | head -n1

# The regression targets exist in the corpus: core functions whose embedded documentation links to a standalone
# docs page with a source-relative path that names neither an entity nor a known section. `toWeek` and `like`
# are core functions, present even in the minimal `Fast test` build (`ENABLE_LIBRARIES=0`).
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0
    FROM system.documentation
    WHERE type = 'Function' AND name = 'toWeek'
      AND position(description, 'type-conversion-functions.md#parseDateTime64BestEffort') > 0"
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0
    FROM system.documentation
    WHERE type = 'Function' AND name = 'like'
      AND position(description, '../syntax.md#string') > 0"
