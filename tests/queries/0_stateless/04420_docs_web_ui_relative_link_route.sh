#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The built-in `/docs` page resolves a relative documentation link that does not point to a documented
# entity against `https://clickhouse.com/docs`. Some embedded entries use source-relative cross-section
# links, for example `mortonEncode` links to `../data-types/int-uint.md`. Simply stripping the leading
# `../` and appending to the docs root produced the broken `/docs/data-types/int-uint`, while the
# working route is `/docs/sql-reference/data-types/int-uint`. `toDocsURL` therefore resolves the first
# path segment to its canonical section route (`DOCS_SECTION_ROUTE` / `DOCS_ROUTE_ROOTS`).

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs")"

# The page is served.
echo "$PAGE" | grep -oF 'ClickHouse <span class="accent">Reference</span>' | head -n1

# `toDocsURL` maps a relative section directory to its canonical docs route ...
echo "$PAGE" | grep -oF 'const DOCS_SECTION_ROUTE = {' | head -n1
echo "$PAGE" | grep -oF "'data-types': '/sql-reference/data-types'," | head -n1
# ... and treats an already-rooted leading segment as a full route.
echo "$PAGE" | grep -oF 'const DOCS_ROUTE_ROOTS = new Set([' | head -n1

# The regression target exists in the corpus: an entity whose embedded documentation links to another
# section with a source-relative path (`../data-types/int-uint.md`). `mortonEncode` is a core function,
# so it is present even in the minimal `Fast test` build (`ENABLE_LIBRARIES=0`).
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0
    FROM system.documentation
    WHERE type = 'Function' AND name = 'mortonEncode'
      AND position(description, '../data-types/int-uint.md') > 0"
