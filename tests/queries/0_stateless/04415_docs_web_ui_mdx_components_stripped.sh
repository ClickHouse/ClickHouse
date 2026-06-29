#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The built-in `/docs` page renders Markdown from `system.documentation`. The embedded documentation
# of website-facing entities (in particular experimental features) carries MDX machinery: `import`
# statements pulling in components such as `<ExperimentalBadge/>` or `<CloudNotSupportedBadge/>`.
# A self-closing custom tag like `<ExperimentalBadge/>` is parsed by the HTML parser as an *unclosed*
# element (the self-closing slash is ignored for non-void elements), so it swallows the rest of the
# document as its children; the sanitizer then drops that whole subtree and the entity renders as an
# empty page. `preprocessMarkdown` must therefore strip the imported components, not only the imports.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs")"

# The page is served.
echo "$PAGE" | grep -oF 'ClickHouse <span class="accent">Reference</span>' | head -n1

# `preprocessMarkdown` collects the names of imported MDX components ...
echo "$PAGE" | grep -oF 'const components = new Set(MDX_COMPONENTS);' | head -n1
echo "$PAGE" | grep -oF 'md.matchAll(importRe)' | head -n1

# ... and strips the opening/closing/self-closing tags of those components from the body.
echo "$PAGE" | grep -oF "out.replace(new RegExp('</?' + name" | head -n1

# The regression target exists in the corpus: an experimental entity whose embedded documentation
# both imports an MDX component and uses it as a self-closing tag (so the empty-page bug could occur).
# We pick the `TimeSeries` table engine because it is registered unconditionally, so it is present
# even in the minimal `Fast test` build (`ENABLE_LIBRARIES=0`); a library-gated entity such as the
# `MaterializedPostgreSQL` database engine would be absent there and the check would never run.
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0
    FROM system.documentation
    WHERE type = 'Table Engine' AND name = 'TimeSeries'
      AND match(description, 'import\\s+ExperimentalBadge')
      AND match(description, '<ExperimentalBadge\\s*/>')"
