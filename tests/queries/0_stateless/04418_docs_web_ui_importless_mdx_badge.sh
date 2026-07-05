#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The built-in `/docs` page renders Markdown from `system.documentation`. Some embedded entries open
# with an MDX status badge that the text does not `import` locally (for example `transactionID`,
# `transactionLatestSnapshot`, and `transactionOldestSnapshot` start with a bare `<ExperimentalBadge/>`
# / `<CloudNotSupportedBadge/>`). A self-closing custom tag like `<ExperimentalBadge/>` is parsed by
# the HTML parser as an *unclosed* non-void element that swallows the rest of the document; the
# sanitizer then drops that whole subtree and the entity renders as an empty page. `preprocessMarkdown`
# therefore strips a known allowlist of MDX components (`MDX_COMPONENTS`) whether or not they were
# imported, not only the imported ones.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs")"

# The page is served.
echo "$PAGE" | grep -oF 'ClickHouse <span class="accent">Reference</span>' | head -n1

# `preprocessMarkdown` seeds its strip set with the known MDX component names ...
echo "$PAGE" | grep -oF 'const MDX_COMPONENTS = [' | head -n1
echo "$PAGE" | grep -oF "'ExperimentalBadge', 'BetaBadge', 'DeprecatedBadge'," | head -n1
# ... so they are stripped even without a local import.
echo "$PAGE" | grep -oF 'const components = new Set(MDX_COMPONENTS);' | head -n1

# The regression target exists in the corpus: an entity whose embedded documentation uses MDX badges
# as self-closing tags *without* importing them (so the empty-page bug could occur). `transactionID`
# is a core function, so it is present even in the minimal `Fast test` build (`ENABLE_LIBRARIES=0`).
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0
    FROM system.documentation
    WHERE type = 'Function' AND name = 'transactionID'
      AND match(description, '<ExperimentalBadge\\s*/>')
      AND match(description, '<CloudNotSupportedBadge\\s*/>')
      AND NOT match(description, 'import\\s+ExperimentalBadge')"
