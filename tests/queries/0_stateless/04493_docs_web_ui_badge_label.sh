#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The built-in `/docs` page renders Markdown from `system.documentation`. Some embedded entries open
# with an MDX status badge component such as `<ExperimentalBadge/>`, `<CloudNotSupportedBadge/>`, or
# `<ScalePlanFeatureBadge .../>`. Unlike decorative machinery, a status badge carries real
# availability information (experimental, not supported in ClickHouse Cloud, plan-gated), so dropping
# it loses that warning from the browser help surface. `preprocessMarkdown` therefore renders any
# `*Badge` component as a readable bold label via `badgeLabel`, using the same labels as the terminal
# `help` renderer (`src/Client/TerminalMarkdownRenderer.cpp`), instead of stripping it. Matching by the
# `*Badge` name (not the local `import`) also handles a badge the website adds later and that an entry
# uses without a local import, and prevents a self-closing badge from swallowing the rest of the
# document into its subtree (which the sanitizer would then drop, rendering the entry empty).

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs")"

# The page is served.
echo "$PAGE" | grep -oF 'ClickHouse <span class="accent">Reference</span>' | head -n1

# `preprocessMarkdown` renders each `*Badge` as a readable label via `badgeLabel` ...
echo "$PAGE" | grep -oF 'function badgeLabel(name) {' | head -n1
echo "$PAGE" | grep -oF "'**[' + badgeLabel(name) + ']**'" | head -n1
# ... using the same labels as the terminal `help` renderer.
echo "$PAGE" | grep -oF "case 'CloudNotSupportedBadge': return 'Not supported in ClickHouse Cloud';" | head -n1

# The regression target is the `S3Queue` engine documentation, which opens with a `<ScalePlanFeatureBadge>`.
# It requires S3 support, so it is absent from the minimal `Fast test` build (`ENABLE_LIBRARIES=0`); the
# check is therefore tolerant of its absence and asserts only that, when present, the badge is still there
# (so the regression input has not silently disappeared) in the raw `system.documentation` content.
$CLICKHOUSE_CLIENT --query "
    SELECT count() = countIf(position(description, '<ScalePlanFeatureBadge') > 0)
    FROM system.documentation
    WHERE type = 'Table Engine' AND name = 'S3Queue'"
