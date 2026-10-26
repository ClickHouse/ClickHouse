#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Some converted documentation pages `import` a common snippet (a settings table, a data-type
# mapping) and use it as a self-closing tag, e.g. `<PrettyFormatSettings/>`. Unlike a decorative
# badge, dropping such a tag loses real content, so the built-in `/docs` page resolves a known
# snippet import to its actual content (see `DOC_SNIPPETS` in `programs/server/docs.html`) instead
# of stripping it like an unrecognized self-closing component.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs")"

# The page is served.
echo "$PAGE" | grep -oF 'ClickHouse <span class="accent">Reference</span>' | head -n1

# The known-snippet resolution table and its lookup are present ...
echo "$PAGE" | grep -oF 'const DOC_SNIPPETS = {' | head -n1
echo "$PAGE" | grep -oF 'snippetContentByLocalName.set(id[1], snippetEntry[1]);' | head -n1

# ... and the actual content of a common snippet is embedded (not merely referenced), so a
# self-closing usage can be resolved without fetching anything at render time.
echo "$PAGE" | grep -oF 'output_format_pretty_max_rows' | head -n1
echo "$PAGE" | grep -oF 'When to use the `JSON` Type' | head -n1

# The regression targets exist in the corpus: the `Pretty` format imports and uses the common
# Pretty settings snippet, and the `JSON` data type imports and uses the when-to-use-JSON snippet.
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0
    FROM system.documentation
    WHERE type = 'Format' AND name = 'Pretty'
      AND description LIKE '%/snippets/common-pretty-format-settings.mdx%'
      AND description LIKE '%<PrettyFormatSettings/>%'"
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0
    FROM system.documentation
    WHERE type = 'Data Type' AND name = 'JSON'
      AND description LIKE '%/snippets/_when-to-use-json.mdx%'
      AND description LIKE '%<WhenToUseJson />%'"
