#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The built-in `/docs` page renders Markdown from `system.documentation`. Some embedded entries open
# with an MDX status badge component. A self-closing custom tag like `<ScalePlanFeatureBadge .../>` is
# parsed by the HTML parser as an *unclosed* non-void element that swallows the rest of the document;
# the sanitizer then drops that whole subtree and the entry renders empty or truncated. The known
# badges in `MDX_COMPONENTS`, and any badge brought in by a local `import`, are stripped, but that
# leaves a badge the website adds later and that an entry uses without a local import unhandled.
# `preprocessMarkdown` therefore also strips any `*Badge` component generically, whether or not it is
# imported or in the known list — the same way the terminal Markdown renderer does. The `S3Queue` engine
# docs open with `<ScalePlanFeatureBadge .../>`, the regression target for the generic rule.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs")"

# The page is served.
echo "$PAGE" | grep -oF 'ClickHouse <span class="accent">Reference</span>' | head -n1

# `preprocessMarkdown` strips any `*Badge` component generically ...
echo "$PAGE" | grep -oF 'Badge(?:\s[^>]*)?\/?>/g' | head -n1
# ... matching the terminal Markdown renderer.
echo "$PAGE" | grep -oF 'terminal Markdown renderer strips' | head -n1

# The regression target is the `S3Queue` engine documentation, which opens with a `<ScalePlanFeatureBadge>`.
# It requires S3 support, so it is absent from the minimal `Fast test` build (`ENABLE_LIBRARIES=0`); the
# check is therefore tolerant of its absence and asserts only that, when present, the badge is still there
# (so the regression input has not silently disappeared).
$CLICKHOUSE_CLIENT --query "
    SELECT count() = countIf(position(description, '<ScalePlanFeatureBadge') > 0)
    FROM system.documentation
    WHERE type = 'Table Engine' AND name = 'S3Queue'"
