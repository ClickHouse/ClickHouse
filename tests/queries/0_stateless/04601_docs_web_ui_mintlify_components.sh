#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Embedded documentation pages converted from the website's Mintlify sources carry Mintlify MDX
# components: `<Note>`/`<Warning>`/... admonitions, `<Tabs>`/`<Tab title="...">` syntax variants,
# `<Card title="...">` callouts, and self-closing snippet references such as `<WhenToUseJson />`.
# The built-in `/docs` page must not lose that content: an unknown element is dropped by the
# sanitizer *with its whole subtree*, so before rendering, `preprocessMarkdown` maps admonition
# components onto the `:::type` syntax (rendered by the admonition extension), keeps `Tab`/`Card`
# titles as bold lines, and strips the remaining component tags while preserving their children.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs")"

# The page is served.
echo "$PAGE" | grep -oF 'ClickHouse <span class="accent">Reference</span>' | head -n1

# Mintlify admonition components are mapped onto the `:::` admonition syntax ...
echo "$PAGE" | grep -oF "':::' + name.toLowerCase()" | head -n1

# ... `Tab`/`TabItem`/`Card` titles survive as bold lines ...
echo "$PAGE" | grep -oF "(?:title|label)=" | head -n1

# ... the component names are known to the strip list, so leftover tags are removed while their
# content is kept ...
echo "$PAGE" | grep -oF "'Note', 'Warning', 'Tip', 'Info', 'Check', 'Danger', 'Caution'," | head -n1

# ... and any remaining self-closing PascalCase component (an unimported website snippet) is
# stripped, so it cannot swallow the rest of the document as its subtree.
echo "$PAGE" | grep -oF "replace(/<[A-Z][A-Za-z0-9]*(?:\s[^>]*)?\/>/g" | head -n1

# The regression targets exist in the corpus (all registered unconditionally, so they are present
# even in the minimal `Fast test` build): the `JSON` data type opens with a `<Card title="...">`
# and uses the `<WhenToUseJson />` snippet, and the `file` table function wraps a caveat in `<Note>`.
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0
    FROM system.documentation
    WHERE type = 'Data Type' AND name = 'JSON'
      AND description LIKE '%<Card title=%'
      AND description LIKE '%<WhenToUseJson />%'"
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0
    FROM system.documentation
    WHERE type = 'Table Function' AND name = 'file'
      AND description LIKE '%<Note>%'"
