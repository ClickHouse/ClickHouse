#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The built-in `/docs` documentation search page must serve its renderer assets (Marked and KaTeX)
# from the same origin (the embedded `/js/...` copies), not from a third-party CDN. Loading
# executable third-party code in the ClickHouse HTTP origin, which handles the user's credentials,
# is a trust-boundary violation, so this guards that the CDN links are rewritten and the assets are
# embedded in the binary.

URL="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"

# The page is served.
${CLICKHOUSE_CURL} -sS "${URL}/docs" | grep -oF 'ClickHouse <span class="accent">Reference</span>' | head -n1

# The renderer assets are referenced from the same origin ...
${CLICKHOUSE_CURL} -sS "${URL}/docs" | grep -oF '/js/marked.min.js' | head -n1
${CLICKHOUSE_CURL} -sS "${URL}/docs" | grep -oF '/js/katex.min.js' | head -n1
${CLICKHOUSE_CURL} -sS "${URL}/docs" | grep -oF '/js/katex.min.css' | head -n1

# ... and no third-party CDN code is loaded by the served page (the count of CDN links is zero).
${CLICKHOUSE_CURL} -sS "${URL}/docs" | grep -c 'cdn\.jsdelivr\.net' ||:

# The assets themselves are served from the binary.
${CLICKHOUSE_CURL} -sS "${URL}/js/marked.min.js" | grep -oF 'marked v12' | head -n1
${CLICKHOUSE_CURL} -sS "${URL}/js/katex.min.js" | grep -oF 'katex' | head -n1

# The KaTeX stylesheet has its fonts inlined as data URIs, so it pulls no external font files.
${CLICKHOUSE_CURL} -sS "${URL}/js/katex.min.css" | grep -oF 'data:font/woff2;base64' | head -n1
${CLICKHOUSE_CURL} -sS "${URL}/js/katex.min.css" | grep -c 'url(fonts/' ||:
