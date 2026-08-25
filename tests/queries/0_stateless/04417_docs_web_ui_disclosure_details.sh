#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The built-in `/docs` page renders Markdown from `system.documentation` through a sanitizer that
# drops elements not on its allowlist together with their whole subtree. Some embedded docs wrap a
# note in an ordinary `<details><summary>...</summary>...</details>` disclosure block (for example
# the "Implementation details" of the `uniq` and `uniqCombined` aggregate functions). When `details`
# and `summary` were not on the allowlist, those sections were removed from the rendered page. The
# sanitizer must keep these safe disclosure tags (and the `open` attribute).

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

PAGE="$(${CLICKHOUSE_CURL} -sS "${URL}/docs")"

# The page is served.
echo "$PAGE" | grep -oF 'ClickHouse <span class="accent">Reference</span>' | head -n1

# The sanitizer allowlist includes the disclosure tags ...
echo "$PAGE" | grep -oF "'details', 'summary'," | head -n1
# ... and permits the `open` attribute.
echo "$PAGE" | grep -oF "'href', 'src', 'alt', 'target', 'rel', 'open'," | head -n1

# The regression target exists in the corpus: an entity whose embedded documentation uses a
# `<details><summary>Implementation details</summary>` disclosure block (so the drop-with-subtree bug
# could occur). The `uniq` aggregate function is a core function, so it is present even in the minimal
# `Fast test` build (`ENABLE_LIBRARIES=0`).
$CLICKHOUSE_CLIENT --query "
    SELECT count() > 0
    FROM system.documentation
    WHERE type = 'Aggregate Function' AND name = 'uniq'
      AND position(description, '<details>') > 0
      AND position(description, '<summary>Implementation details</summary>') > 0"
