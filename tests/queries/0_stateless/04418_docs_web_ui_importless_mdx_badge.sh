#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The built-in `/docs` page renders Markdown from `system.documentation`. Some embedded entries open
# with an MDX status badge that the text does not `import` locally (for example `transactionID`,
# `transactionLatestSnapshot`, and `transactionOldestSnapshot` start with a bare `<ExperimentalBadge/>`
# / `<CloudNotSupportedBadge/>`). A self-closing custom tag like `<ExperimentalBadge/>` is otherwise
# parsed by the HTML parser as an *unclosed* non-void element that swallows the rest of the document;
# the sanitizer then drops that whole subtree and the entity renders as an empty page. Such a badge
# also carries real availability information, so `preprocessMarkdown` renders any `*Badge` component as
# a readable label via `badgeLabel` (see the `04493` test). It matches the `*Badge` name directly, not
# a collected `import` name, so an importless badge is handled just like an imported one.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs")"

# The page is served.
echo "$PAGE" | grep -oF 'ClickHouse <span class="accent">Reference</span>' | head -n1

# `preprocessMarkdown` renders each `*Badge` as a readable label via `badgeLabel` ...
echo "$PAGE" | grep -oF 'function badgeLabel(name) {' | head -n1
# ... matching the `*Badge` name directly, so an importless badge is handled the same as an imported one.
echo "$PAGE" | grep -oF '[A-Z][A-Za-z0-9]*Badge' | head -n1

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
