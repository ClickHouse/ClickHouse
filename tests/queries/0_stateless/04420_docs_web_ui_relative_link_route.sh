#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The built-in `/docs` page retains support for legacy relative documentation links by resolving their
# first path segment to its canonical section route (`DOCS_SECTION_ROUTE` / `DOCS_ROUTE_ROOTS`).
# Current embedded documentation uses site-root absolute routes instead, so it does not depend on this
# compatibility mapping.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs")"

# The page is served.
echo "$PAGE" | grep -oF 'ClickHouse <span class="accent">Reference</span>' | head -n1

# `toDocsURL` maps a relative section directory to its canonical docs route ...
echo "$PAGE" | grep -oF 'const DOCS_SECTION_ROUTE = {' | head -n1
echo "$PAGE" | grep -oF "'data-types': '/sql-reference/data-types'," | head -n1
# ... and treats an already-rooted leading segment as a full route.
echo "$PAGE" | grep -oF 'const DOCS_ROUTE_ROOTS = new Set([' | head -n1
# The page-load compatibility assertion feeds the original relative link to `toDocsURL`.
echo "$PAGE" | grep -oF "'../data-types/int-uint.md': 'https://clickhouse.com/docs/sql-reference/data-types/int-uint'," | head -n1

# `mortonEncode` is a core function, so it is present even in the minimal `Fast test` build
# (`ENABLE_LIBRARIES=0`). Its embedded documentation uses the current site-root absolute route and no
# longer contains the legacy `../data-types/int-uint.md` link.
$CLICKHOUSE_CLIENT --query "
    SELECT
        position(description, '/reference/data-types/int-uint') > 0
            AND position(description, '../data-types/int-uint.md') = 0
    FROM system.documentation
    WHERE type = 'Function' AND name = 'mortonEncode'"
