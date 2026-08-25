#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The built-in `/docs` page rewrites a relative documentation link that does not point to a documented
# entity into a `https://clickhouse.com/docs` URL. Two more shapes from the current corpus used to fall
# back to the bare docs root, losing the target page and its `#anchor`:
#   * a settings page linked with the extension and a stray slash before the fragment, e.g.
#     `materialize_skip_indexes_on_insert` links to `merge-tree-settings.md/#materialize_skip_indexes_on_merge`;
#   * a section overview linked with a source-relative path, e.g. `regionToPopulation` links to
#     `../dictionaries#embedded-dictionaries`.
# `toDocsURL` now normalizes the stray trailing slash before stripping the extension, maps the
# `merge-tree-settings` page in `DOCS_PAGE_ROUTE`, and maps the `dictionaries` section in
# `DOCS_SECTION_ROUTE`, so both resolve to their canonical route while keeping the `#fragment`.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs")"

# The page is served.
echo "$PAGE" | grep -oF 'ClickHouse <span class="accent">Reference</span>' | head -n1

# `toDocsURL` drops a trailing slash before stripping the `.md`/`.mdx` extension ...
echo "$PAGE" | grep -oF 'so drop a trailing slash before stripping the extension.' | head -n1
# ... maps the `dictionaries` section to its canonical route ...
echo "$PAGE" | grep -oF "'dictionaries': '/sql-reference/dictionaries'," | head -n1
# ... and maps the standalone `merge-tree-settings` page to its canonical route.
echo "$PAGE" | grep -oF "'merge-tree-settings': '/operations/settings/merge-tree-settings'," | head -n1

# The regression targets exist in the corpus. `materialize_skip_indexes_on_insert` is a core setting and
# `regionToPopulation` is a core embedded-dictionary function, so both are present even in the minimal
# `Fast test` build (`ENABLE_LIBRARIES=0`).
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0
    FROM system.documentation
    WHERE type = 'Setting' AND name = 'materialize_skip_indexes_on_insert'
      AND position(description, 'merge-tree-settings.md/#materialize_skip_indexes_on_merge') > 0"
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0
    FROM system.documentation
    WHERE type = 'Function' AND name = 'regionToPopulation'
      AND position(description, '../dictionaries#embedded-dictionaries') > 0"
