#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The built-in `/docs` page retains compatibility mappings for legacy relative links to settings pages
# and section overviews. Current embedded documentation uses site-root absolute routes instead, so it
# preserves the intended page and anchor without depending on these mappings.

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
# The page-load compatibility assertion feeds both original link shapes to the real `toDocsURL`.
echo "$PAGE" | grep -oF "'merge-tree-settings.md/#materialize_skip_indexes_on_merge': 'https://clickhouse.com/docs/operations/settings/merge-tree-settings#materialize_skip_indexes_on_merge'," | head -n1
echo "$PAGE" | grep -oF "'../dictionaries#embedded-dictionaries': 'https://clickhouse.com/docs/sql-reference/dictionaries#embedded-dictionaries'," | head -n1
echo "$PAGE" | grep -oF 'verifyDocsURLCompatibility();' | head -n1

# `materialize_skip_indexes_on_insert` is a core setting and `regionToPopulation` is a core
# embedded-dictionary function, so both are present even in the minimal `Fast test` build
# (`ENABLE_LIBRARIES=0`). Their descriptions use current site-root absolute routes and no longer contain
# the legacy relative forms.
$CLICKHOUSE_CLIENT --query "
    SELECT
        position(description, '/reference/settings/merge-tree-settings/materialize#materialize_skip_indexes_on_merge') > 0
            AND position(description, 'merge-tree-settings.md/#materialize_skip_indexes_on_merge') = 0
    FROM system.documentation
    WHERE type = 'Setting' AND name = 'materialize_skip_indexes_on_insert'"
$CLICKHOUSE_CLIENT --query "
    SELECT
        position(description, '/reference/statements/create/dictionary/embedded') > 0
            AND position(description, '../dictionaries#embedded-dictionaries') = 0
    FROM system.documentation
    WHERE type = 'Function' AND name = 'regionToPopulation'"
